const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ML_CLIENT_ID = process.env.ML_CLIENT_ID;
const ML_CLIENT_SECRET = process.env.ML_CLIENT_SECRET;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !ML_CLIENT_ID || !ML_CLIENT_SECRET) {
  throw new Error('Missing required environment variables');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function getLatestToken() {
  const { data, error } = await supabase
    .from('meli_oauth_tokens')
    .select('user_id, access_token, refresh_token, token_type, scope, expires_at, updated_at, created_at')
    .order('updated_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (!data || !data.access_token || !data.refresh_token) {
    throw new Error('No token found in meli_oauth_tokens (missing access_token or refresh_token)');
  }
  return data;
}

async function refreshToken(refresh_token) {
  const res = await fetch('https://api.mercadolibre.com/oauth/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: ML_CLIENT_ID,
      client_secret: ML_CLIENT_SECRET,
      refresh_token,
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`Refresh token failed: ${res.status} ${JSON.stringify(json)}`);
  }

  const access_token = String(json.access_token);
  const new_refresh_token = String(json.refresh_token ?? refresh_token);
  const expires_in = Number(json.expires_in ?? 21600);
  const expires_at = new Date(Date.now() + expires_in * 1000).toISOString();

  // Upsert by user_id if provided, else update by refresh_token
  if (json.user_id) {
    const user_id = String(json.user_id);
    const { error } = await supabase.from('meli_oauth_tokens').upsert({
      user_id,
      access_token,
      refresh_token: new_refresh_token,
      token_type: json.token_type ?? 'Bearer',
      scope: json.scope,
      expires_at,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'user_id' });
    if (error) throw error;
  } else {
    const { error } = await supabase
      .from('meli_oauth_tokens')
      .update({
        access_token,
        refresh_token: new_refresh_token,
        token_type: json.token_type ?? 'Bearer',
        scope: json.scope,
        expires_at,
        updated_at: new Date().toISOString(),
      })
      .eq('refresh_token', refresh_token);
    if (error) throw error;
  }
  return { access_token, refresh_token: new_refresh_token, expires_at };
}

async function mlGetPublic(url) {
  return await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'KelokeTrendsBot/1.0',
    },
  });
}

async function mlGetPrivate(url, accessToken) {
  return await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: 'application/json',
      'User-Agent': 'KelokeTrendsBot/1.0',
    },
  });
}

function extractItems(category_id, payload) {
  if (Array.isArray(payload?.results) && payload.results.length > 0) {
    return payload.results;
  }
  if (Array.isArray(payload) && payload.length > 0) return payload;
  return [];
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Method not allowed' });
    return;
  }

  const body = req.body || {};
  const site_id = String(body.site_id ?? (body.country === 'CL' ? 'MLC' : 'MLC'));
  const batch = Number(body.batch ?? 5);
  const limit = Number(body.limit ?? 50);

  try {
    const { data: jobs, error: jobsErr } = await supabase
      .from('meli_scan_jobs')
      .select('id, site_id, category_id, status, attempts')
      .eq('status', 'pending')
      .eq('site_id', site_id)
      .order('id', { ascending: true })
      .limit(batch);

    if (jobsErr) {
      res.status(500).json({ ok: false, error: jobsErr.message });
      return;
    }
    if (!jobs || jobs.length === 0) {
      res.json({ ok: true, msg: 'no_jobs', site_id, batch, limit });
      return;
    }

    let token_source = 'none';
    let accessToken = '';
    let refreshTok = '';

    const incomingAuth = req.headers['authorization'] || req.headers['Authorization'] || '';
    const incomingBearer = (incomingAuth || '').trim();
    if (incomingBearer) {
      token_source = 'header';
      accessToken = incomingBearer.toLowerCase().startsWith('bearer ')
        ? incomingBearer.slice(7).trim()
        : incomingBearer;
      const tok = await getLatestToken();
      refreshTok = String(tok.refresh_token);
    } else {
      const tok = await getLatestToken();
      token_source = 'db';
      accessToken = String(tok.access_token);
      refreshTok = String(tok.refresh_token);
    }

    const results = [];
    let processed = 0;
    let inserted = 0;

    for (const job of jobs) {
      processed++;

      await supabase
        .from('meli_scan_jobs')
        .update({
          status: 'processing',
          attempts: (job.attempts ?? 0) + 1,
          updated_at: new Date().toISOString(),
          last_error: null,
        })
        .eq('id', job.id);

      const isSeller = String(job.category_id).startsWith('SELLER:');
      let url;
      if (isSeller) {
        const sellerId = String(job.category_id).replace('SELLER:', '').trim();
        url = `https://api.mercadolibre.com/users/${encodeURIComponent(sellerId)}/items/search?limit=${encodeURIComponent(String(limit))}`;
      } else {
        url = `https://api.mercadolibre.com/sites/${encodeURIComponent(site_id)}/search?category=${encodeURIComponent(String(job.category_id))}&limit=${encodeURIComponent(String(limit))}`;
      }

      let resMl = await mlGetPublic(url);
      if (isSeller && resMl.status === 401) {
        const refreshed = await refreshToken(refreshTok);
        accessToken = refreshed.access_token;
        refreshTok = refreshed.refresh_token;
        resMl = await mlGetPrivate(url, accessToken);
      }

      const payload = await resMl.json().catch(() => ({}));

      if (!resMl.ok) {
        await supabase
          .from('meli_scan_jobs')
          .update({
            status: 'error',
            last_error: `ML ${resMl.status} ${JSON.stringify(payload)}`,
            updated_at: new Date().toISOString(),
          })
          .eq('id', job.id);

        results.push({ job_id: job.id, category: job.category_id, ok: false, status: resMl.status, payload });
        continue;
      }

      const items = extractItems(job.category_id, payload);

      if (items.length > 0) {
        const rows = items.map((it) => {
          const isStringId = typeof it === 'string';
          return {
            job_id: job.id,
            site_id,
            category_id: job.category_id,
            item_id: String(isStringId ? it : it.id),
            title: isStringId ? null : it.title ?? null,
            permalink: isStringId ? null : it.permalink ?? null,
            price: isStringId ? null : it.price ?? null,
            currency_id: isStringId ? null : it.currency_id ?? null,
            seller_id: isStringId ? null : (it.seller?.id ?? it.seller_id ?? null),
            raw: it,
          };
        });

        const { error: upErr } = await supabase
          .from('meli_category_items')
          .upsert(rows, { onConflict: 'site_id,item_id' });

        if (upErr) {
          await supabase
            .from('meli_scan_jobs')
            .update({
              status: 'error',
              last_error: `DB upsert error: ${upErr.message}`,
              updated_at: new Date().toISOString(),
            })
            .eq('id', job.id);

          results.push({ job_id: job.id, category: job.category_id, ok: false, db_error: upErr.message });
          continue;
        }

        inserted += rows.length;
      }

      await supabase
        .from('meli_scan_jobs')
        .update({
          status: 'done',
          updated_at: new Date().toISOString(),
          last_error: null,
        })
        .eq('id', job.id);

      results.push({
        job_id: job.id,
        category: job.category_id,
        ok: true,
        items: items.length,
        mode: isSeller ? 'seller' : 'category_public',
      });
    }

    res.json({
      ok: true,
      site_id,
      processed,
      inserted,
      token_source,
      token_len: accessToken.length,
      results,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || String(err) });
  }
};
