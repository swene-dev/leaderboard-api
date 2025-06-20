import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { Agent } from '@openserv-labs/sdk';
import { subDays, subMinutes } from 'date-fns';
import { Pool } from 'pg';
import 'dotenv/config';

// Type definitions
interface Tweet {
  id: string;
  username: string;
  follower_count: number;
  created_at: string;
  likes: number;
  retweets: number;
  text: string;
  url: string;
}

interface UserStats {
  username: string;
  totalScore: number;
  tweetCount: number;
  avgLikes: number;
  avgRetweets: number;
  topTweet: Tweet;
  lastActive: string;
}

interface LeaderboardData {
  week: string;
  users: UserStats[];
  totalTweets: number;
  mostViralTweet: Tweet;
}

interface TwitterResponse {
  data?: any[];
  includes?: {
    users?: any[];
  };
  meta?: {
    next_token?: string;
  };
}

interface ApiResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
  error?: string;
}

// Configuration
const CONFIG = {
  OPENSERV_API_KEY: process.env.OPENSERV_API_KEY || '',
  WORKSPACE_ID: parseInt(process.env.WORKSPACE_ID || '0', 10),
  TWITTER_INTEGRATION_ID: process.env.TWITTER_INTEGRATION_ID || 'twitter-v2',
  API_KEY: process.env.API_KEY || '',
  DATABASE_URL: process.env.DATABASE_URL || '',
  NODE_ENV: process.env.NODE_ENV || 'development',
};

// Validate required environment variables
if (!CONFIG.OPENSERV_API_KEY || !CONFIG.WORKSPACE_ID || !CONFIG.DATABASE_URL) {
  console.error('❌ Missing required environment variables: OPENSERV_API_KEY, WORKSPACE_ID, DATABASE_URL');
  if (CONFIG.NODE_ENV !== 'development') {
    process.exit(1);
  }
}

// PostgreSQL connection pool
const pool = new Pool({
  connectionString: CONFIG.DATABASE_URL,
  ssl: CONFIG.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Initialize Express app
const app = express();

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  message: { success: false, error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter);

// API key authentication middleware
const authenticateApiKey = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (!CONFIG.API_KEY) {
    return next(); // Skip authentication if no API key is set
  }
  
  const apiKey = req.headers['x-api-key'] || req.query.api_key;
  if (!apiKey || apiKey !== CONFIG.API_KEY) {
    return res.status(401).json({ success: false, error: 'Invalid or missing API key' });
  }
  
  next();
};

// Initialize OpenServ agent
const twitterAgent = new Agent({
  systemPrompt: 'You are a Twitter integration agent for fetching OpenServ mentions and building community leaderboards.',
  apiKey: CONFIG.OPENSERV_API_KEY,
});

// Database initialization
async function initializeDatabase() {
  try {
    // Create tweets table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tweets (
        id VARCHAR(255) PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        follower_count INTEGER DEFAULT 0,
        created_at TIMESTAMP NOT NULL,
        likes INTEGER DEFAULT 0,
        retweets INTEGER DEFAULT 0,
        text TEXT,
        url VARCHAR(500),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create leaderboards table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS leaderboards (
        id SERIAL PRIMARY KEY,
        week VARCHAR(20) UNIQUE NOT NULL,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create system_info table for tracking last fetch time
    await pool.query(`
      CREATE TABLE IF NOT EXISTS system_info (
        key VARCHAR(100) PRIMARY KEY,
        value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create indexes for better performance
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username);
      CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at);
      CREATE INDEX IF NOT EXISTS idx_leaderboards_week ON leaderboards(week);
    `);

    console.log('✅ Database initialized successfully');
  } catch (error) {
    console.error('❌ Error initializing database:', error);
    throw error;
  }
}

// Database helper functions
class DatabaseService {
  static async getTweets(weekOffset: number = 0): Promise<Tweet[]> {
    const now = new Date();
    const weekStart = subDays(now, 7 + (weekOffset * 7));
    const weekEnd = subDays(now, weekOffset * 7);

    const result = await pool.query(
      'SELECT * FROM tweets WHERE created_at >= $1 AND created_at < $2 ORDER BY created_at DESC',
      [weekStart.toISOString(), weekEnd.toISOString()]
    );

    return result.rows.map(row => ({
      id: row.id,
      username: row.username,
      follower_count: row.follower_count,
      created_at: row.created_at.toISOString(),
      likes: row.likes,
      retweets: row.retweets,
      text: row.text,
      url: row.url
    }));
  }

  static async insertTweets(tweets: Tweet[]): Promise<void> {
    if (tweets.length === 0) return;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      
      for (const tweet of tweets) {
        await client.query(
          `INSERT INTO tweets (id, username, follower_count, created_at, likes, retweets, text, url) 
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`,
          [tweet.id, tweet.username, tweet.follower_count, tweet.created_at, tweet.likes, tweet.retweets, tweet.text, tweet.url]
        );
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  static async getLeaderboard(week: string): Promise<LeaderboardData | null> {
    const result = await pool.query('SELECT data FROM leaderboards WHERE week = $1', [week]);
    return result.rows[0]?.data || null;
  }

  static async saveLeaderboard(week: string, data: LeaderboardData): Promise<void> {
    await pool.query(
      `INSERT INTO leaderboards (week, data, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)
       ON CONFLICT (week) DO UPDATE SET data = $2, updated_at = CURRENT_TIMESTAMP`,
      [week, JSON.stringify(data)]
    );
  }

  static async getHistoricalLeaderboards(limit: number = 5): Promise<LeaderboardData[]> {
    const result = await pool.query(
      'SELECT data FROM leaderboards ORDER BY week DESC LIMIT $1',
      [limit]
    );
    return result.rows.map(row => row.data);
  }

  static async getSystemInfo(key: string): Promise<string | null> {
    const result = await pool.query('SELECT value FROM system_info WHERE key = $1', [key]);
    return result.rows[0]?.value || null;
  }

  static async setSystemInfo(key: string, value: string): Promise<void> {
    await pool.query(
      `INSERT INTO system_info (key, value, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)
       ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = CURRENT_TIMESTAMP`,
      [key, value]
    );
  }

  static async getTotalTweetsCount(): Promise<number> {
    const result = await pool.query('SELECT COUNT(*) as count FROM tweets');
    return parseInt(result.rows[0]?.count || '0', 10);
  }
}

// Type guard and utility functions
function isValidTweet(tweet: any): tweet is Tweet {
  return tweet && 
         typeof tweet === 'object' && 
         typeof tweet.id === 'string' && 
         typeof tweet.username === 'string' && 
         tweet.username !== 'unknown' && 
         tweet.username !== '';
}

function safeNumber(value: any, defaultValue: number = 0): number {
  if (value === null || value === undefined) return defaultValue;
  const num = Number(value);
  return isNaN(num) ? defaultValue : num;
}

function getWeekKey(date: Date = new Date()): string {
  const weekStart = new Date(date);
  weekStart.setDate(date.getDate() - date.getDay());
  return weekStart.toISOString().split('T')[0] || '';
}

function calculateUserScore(userTweets: Tweet[], previousWeekUsers: string[]): number {
  const sortedTweets = [...userTweets].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
  let totalScore = 0;
  const username = userTweets[0]?.username;

  for (let i = 0; i < sortedTweets.length; i++) {
    const tweet = sortedTweets[i];
    if (!tweet) continue;

    const impactScore = (tweet.follower_count * 0.001) + tweet.likes + (tweet.retweets * 2);
    const freshnessMultiplier = i === 0 ? 3 : i === 1 ? 2 : 1;
    const decayFactor = i === 0 ? 1 : i === 1 ? 0.5 : 0.25;
    
    totalScore += impactScore * freshnessMultiplier * decayFactor;
  }

  const consistencyMultiplier = username && previousWeekUsers.includes(username) ? 1.25 : 1;
  return totalScore * consistencyMultiplier;
}

// Core business logic functions
async function fetchTwitterMentions(startTime: string, endTime: string, maxResults: number = 100): Promise<Tweet[]> {
  let allTweets: any[] = [];
  let allUsers: any[] = [];
  let nextToken: string | undefined;
  const maxPages = 10;
  let pageCount = 0;

  try {
    do {
      pageCount++;
      console.log(`Fetching page ${pageCount}...`);

      const searchQuery = encodeURIComponent('("OpenServ" OR "@OpenServAI" OR "#OpenServ" OR "openserv.ai") -is:retweet');
      const tweetFields = encodeURIComponent('created_at,public_metrics,author_id,text');
      const expansions = encodeURIComponent('author_id');
      const userFields = encodeURIComponent('username,public_metrics');

      let endpoint = `/2/tweets/search/recent?query=${searchQuery}&start_time=${encodeURIComponent(startTime)}&end_time=${encodeURIComponent(endTime)}&max_results=${maxResults}&sort_order=recency&tweet.fields=${tweetFields}&expansions=${expansions}&user.fields=${userFields}`;

      if (nextToken) endpoint += `&next_token=${encodeURIComponent(nextToken)}`;

      const response = await twitterAgent.callIntegration({
        workspaceId: CONFIG.WORKSPACE_ID,
        integrationId: CONFIG.TWITTER_INTEGRATION_ID,
        details: { endpoint, method: 'GET' },
      });

      const responseData: TwitterResponse = typeof response.output === 'string' ? JSON.parse(response.output) : response.output;

      if (responseData.data && Array.isArray(responseData.data)) allTweets.push(...responseData.data);
      if (responseData.includes?.users && Array.isArray(responseData.includes.users)) allUsers.push(...responseData.includes.users);

      nextToken = responseData.meta?.next_token;
      if (nextToken && pageCount < maxPages) await new Promise(r => setTimeout(r, 2000));
    } while (nextToken && pageCount < maxPages);

    console.log(`Found ${allTweets.length} tweets and ${allUsers.length} users`);

    const processedTweets: Tweet[] = allTweets.map(tweet => {
      const author = allUsers.find(user => user.id === tweet.author_id);
      return {
        id: tweet.id,
        username: author?.username || 'unknown',
        follower_count: safeNumber(author?.public_metrics?.follower_count),
        created_at: tweet.created_at,
        likes: safeNumber(tweet.public_metrics?.like_count),
        retweets: safeNumber(tweet.public_metrics?.retweet_count),
        text: tweet.text || '',
        url: `https://twitter.com/${author?.username}/status/${tweet.id}`
      };
    });

    return processedTweets.filter(tweet => tweet.username !== 'unknown');
  } catch (error) {
    console.error('Error fetching tweets:', error);
    throw error;
  }
}

async function fetchAllMentionsHistorical(): Promise<number> {
  console.log('🔄 Fetching historical mentions...');
  
  const now = new Date();
  const sevenDaysAgo = subDays(now, 7);
  const startTime = sevenDaysAgo.toISOString();
  const endTime = new Date(now.getTime() - 30 * 1000).toISOString();

  const tweets = await fetchTwitterMentions(startTime, endTime);
  await DatabaseService.insertTweets(tweets);
  await DatabaseService.setSystemInfo('lastFetch', new Date().toISOString());
  
  console.log(`✅ Fetched ${tweets.length} new tweets`);
  
  if (tweets.length > 0) {
    await scoreAndStore();
  }
  
  return tweets.length;
}

async function fetchLatestMentionsLive(): Promise<number> {
  console.log('🔄 Fetching live mentions...');
  
  const lastFetchStr = await DatabaseService.getSystemInfo('lastFetch');
  const lastFetch = lastFetchStr ? new Date(lastFetchStr) : new Date(0);
  const now = new Date();
  const thirtyMinutesAgo = subMinutes(now, 30);
  
  const startTime = lastFetch > thirtyMinutesAgo ? lastFetch.toISOString() : thirtyMinutesAgo.toISOString();
  const endTime = new Date(now.getTime() - 30 * 1000).toISOString();

  const tweets = await fetchTwitterMentions(startTime, endTime, 50);
  await DatabaseService.insertTweets(tweets);
  await DatabaseService.setSystemInfo('lastFetch', new Date().toISOString());
  
  console.log(`✅ Live fetch: ${tweets.length} new tweets`);
  
  if (tweets.length > 0) {
    await scoreAndStore();
  }
  
  return tweets.length;
}

async function scoreAndStore(): Promise<LeaderboardData> {
  console.log('📊 Scoring and storing leaderboard...');
  
  const currentWeekTweets = await DatabaseService.getTweets(0);
  const previousWeekTweets = await DatabaseService.getTweets(1);
  
  if (currentWeekTweets.length === 0) {
    throw new Error('No tweets found for current week');
  }

  const tweetsByUser: Record<string, Tweet[]> = {};
  
  currentWeekTweets
    .filter(isValidTweet)
    .forEach(tweet => {
      if (!tweetsByUser[tweet.username]) {
        tweetsByUser[tweet.username] = [];
      }
      tweetsByUser[tweet.username]!.push(tweet);
    });

  const previousWeekUsers = [...new Set(previousWeekTweets.map(t => t.username))];

  const userStats: UserStats[] = Object.entries(tweetsByUser)
    .map(([username, tweets]) => {
      if (!tweets || tweets.length === 0) return null;
      
      const score = calculateUserScore(tweets, previousWeekUsers);
      const avgLikes = tweets.reduce((sum, t) => sum + t.likes, 0) / tweets.length;
      const avgRetweets = tweets.reduce((sum, t) => sum + t.retweets, 0) / tweets.length;
      const topTweet = tweets.reduce((best, current) => 
        (current.likes + current.retweets * 2) > (best.likes + best.retweets * 2) ? current : best
      );

      return {
        username,
        totalScore: score,
        tweetCount: tweets.length,
        avgLikes,
        avgRetweets,
        topTweet,
        lastActive: tweets[0]?.created_at || new Date().toISOString()
      };
    })
    .filter((stat): stat is UserStats => stat !== null && stat.totalScore > 0);

  userStats.sort((a, b) => b.totalScore - a.totalScore);

  let mostViralTweet: Tweet;
  if (currentWeekTweets.length > 0) {
    mostViralTweet = currentWeekTweets.reduce((best, current) => 
      (current.likes + current.retweets * 2) > (best.likes + best.retweets * 2) ? current : best
    );
  } else {
    mostViralTweet = {
      id: 'dummy', 
      username: 'none', 
      follower_count: 0, 
      created_at: new Date().toISOString(),
      likes: 0, 
      retweets: 0, 
      text: 'No tweets this week', 
      url: ''
    };
  }

  const leaderboardData: LeaderboardData = {
    week: getWeekKey(),
    users: userStats,
    totalTweets: currentWeekTweets.length,
    mostViralTweet
  };

  await DatabaseService.saveLeaderboard(leaderboardData.week, leaderboardData);
  console.log(`✅ Scored ${userStats.length} users for week ${getWeekKey()}`);
  
  return leaderboardData;
}

// API Routes

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    success: true, 
    message: 'OpenServ Leaderboard API is running',
    timestamp: new Date().toISOString(),
    version: '2.0.0'
  });
});

// Get current leaderboard
app.get('/api/leaderboard', async (req, res) => {
  try {
    const currentWeek = getWeekKey();
    const leaderboard = await DatabaseService.getLeaderboard(currentWeek);
    
    if (!leaderboard) {
      return res.json({
        success: true,
        data: {
          week: currentWeek,
          users: [],
          totalTweets: 0,
          mostViralTweet: null,
          message: 'No leaderboard data available for current week'
        }
      });
    }
    
    res.json({
      success: true,
      data: leaderboard
    });
  } catch (error) {
    console.error('Error getting leaderboard:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve leaderboard'
    });
  }
});

// Get user stats
app.get('/api/user/:username', async (req, res) => {
  try {
    const { username } = req.params;
    const currentWeek = getWeekKey();
    const leaderboard = await DatabaseService.getLeaderboard(currentWeek);
    
    if (!leaderboard) {
      return res.status(404).json({
        success: false,
        error: 'No leaderboard data available'
      });
    }
    
    const userStats = leaderboard.users.find(u => 
      u.username.toLowerCase() === username.toLowerCase()
    );
    
    if (!userStats) {
      return res.status(404).json({
        success: false,
        error: `User @${username} not found in current week's leaderboard`
      });
    }
    
    const rank = leaderboard.users.findIndex(u => 
      u.username.toLowerCase() === username.toLowerCase()
    ) + 1;
    
    res.json({
      success: true,
      data: {
        ...userStats,
        rank,
        week: currentWeek
      }
    });
  } catch (error) {
    console.error('Error getting user stats:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve user stats'
    });
  }
});

// Get analytics
app.get('/api/analytics', async (req, res) => {
  try {
    const currentWeek = getWeekKey();
    const leaderboard = await DatabaseService.getLeaderboard(currentWeek);
    
    if (!leaderboard) {
      return res.status(404).json({
        success: false,
        error: 'No analytics data available'
      });
    }
    
    const totalUsers = leaderboard.users.length;
    const avgScore = totalUsers > 0 ? 
      leaderboard.users.reduce((sum, u) => sum + u.totalScore, 0) / totalUsers : 0;
    
    // Get historical data for trends
    const historicalBoards = await DatabaseService.getHistoricalLeaderboards(4);
    const prevWeek = historicalBoards.find(lb => lb.week !== currentWeek);
    
    let fastestRiser = null;
    if (prevWeek) {
      for (const user of leaderboard.users.slice(0, 5)) {
        const prevRank = prevWeek.users.findIndex(u => u.username === user.username);
        const currentRank = leaderboard.users.findIndex(u => u.username === user.username);
        if (prevRank > currentRank && prevRank >= 0) {
          fastestRiser = {
            username: user.username,
            previousRank: prevRank + 1,
            currentRank: currentRank + 1,
            improvement: prevRank - currentRank
          };
          break;
        }
      }
    }
    
    const analytics = {
      week: currentWeek,
      totalUsers,
      totalTweets: leaderboard.totalTweets,
      avgScore: Math.round(avgScore * 100) / 100,
      fastestRiser,
      mostViralTweet: leaderboard.mostViralTweet,
      topPerformers: leaderboard.users.slice(0, 3),
      weeklyTrend: historicalBoards.slice(-4).map(lb => ({
        week: lb.week,
        users: lb.users.length,
        tweets: lb.totalTweets
      }))
    };
    
    res.json({
      success: true,
      data: analytics
    });
  } catch (error) {
    console.error('Error getting analytics:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve analytics'
    });
  }
});

// Trigger fetch (requires authentication if API key is set)
app.post('/api/fetch', authenticateApiKey, async (req, res) => {
  try {
    const { type = 'historical' } = req.body;
    
    let newTweetsCount: number;
    if (type === 'live') {
      newTweetsCount = await fetchLatestMentionsLive();
    } else {
      newTweetsCount = await fetchAllMentionsHistorical();
    }
    
    res.json({
      success: true,
      data: {
        newTweetsCount,
        message: `Successfully fetched ${newTweetsCount} new tweets`,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Error in fetch endpoint:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch tweets'
    });
  }
});

// Manual scoring trigger (requires authentication if API key is set)
app.post('/api/score', authenticateApiKey, async (req, res) => {
  try {
    const leaderboard = await scoreAndStore();
    
    res.json({
      success: true,
      data: {
        leaderboard,
        message: 'Leaderboard scoring completed successfully'
      }
    });
  } catch (error) {
    console.error('Error in score endpoint:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Failed to score leaderboard'
    });
  }
});

// Get historical leaderboards
app.get('/api/leaderboards', async (req, res) => {
  try {
    const { limit = 5 } = req.query;
    const limitNum = parseInt(limit as string, 10);
    
    const leaderboards = await DatabaseService.getHistoricalLeaderboards(limitNum);
    
    res.json({
      success: true,
      data: {
        leaderboards,
        total: leaderboards.length
      }
    });
  } catch (error) {
    console.error('Error getting historical leaderboards:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve historical leaderboards'
    });
  }
});

// Get system status
app.get('/api/status', async (req, res) => {
  try {
    const currentWeek = getWeekKey();
    const currentLeaderboard = await DatabaseService.getLeaderboard(currentWeek);
    const lastFetch = await DatabaseService.getSystemInfo('lastFetch');
    const totalTweets = await DatabaseService.getTotalTweetsCount();
    
    res.json({
      success: true,
      data: {
        status: 'operational',
        lastFetch: lastFetch || 'Never',
        totalTweets,
        currentWeek,
        currentWeekUsers: currentLeaderboard?.users.length || 0,
        currentWeekTweets: currentLeaderboard?.totalTweets || 0,
        environment: CONFIG.NODE_ENV,
        timestamp: new Date().toISOString(),
        database: 'PostgreSQL'
      }
    });
  } catch (error) {
    console.error('Error getting status:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve system status'
    });
  }
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found'
  });
});

// Error handler
app.use((error: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Unhandled error:', error);
  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
});

// Initialize database on startup
if (CONFIG.DATABASE_URL) {
  initializeDatabase().catch(console.error);
}

// Export for Vercel
export default app;