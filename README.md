# Social Media Listening Analytics Platform

A comprehensive social media sentiment analytics platform that provides real-time, multi-platform sentiment insights with advanced data visualization and analysis capabilities for Marks & Spencer vs Next Retail.

![Dashboard Preview](https://via.placeholder.com/800x400/1a202c/ffffff?text=Social+Media+Analytics+Dashboard)

## 🚀 Features

- **Real-time Analytics** - Live social media metrics from TikTok, Instagram, and YouTube
- **Multi-Platform Comparison** - Compare performance between Marks & Spencer and Next Retail
- **Sentiment Analysis** - Advanced sentiment tracking with authentic data processing
- **Interactive Charts** - Dynamic visualizations with platform-specific filtering
- **Demographic Insights** - Age and gender distribution analytics
- **Content Performance** - Top-performing content analysis across platforms
- **Trending Topics** - Real-time topic extraction and sentiment scoring

## 🛠️ Tech Stack

### Frontend
- **React 18** with TypeScript
- **Tailwind CSS** for styling
- **Shadcn/ui** component library
- **Recharts** for data visualization
- **TanStack Query** for data management
- **Wouter** for routing

### Backend
- **Node.js** with Express.js
- **TypeScript** for type safety
- **Drizzle ORM** for database operations
- **PostgreSQL** (Neon serverless)
- **Passport.js** for authentication

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Node.js** (version 18.0 or higher)
- **npm** (version 8.0 or higher)
- **PostgreSQL** database (or Neon account)
- **Git**

## 🔧 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Ananya786/MarksandSpencers.git
cd MarksandSpencers
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Environment Setup

Create a `.env` file in the root directory:

```env
# Database Configuration
DATABASE_URL=your_postgresql_connection_string
PGHOST=your_pg_host
PGPORT=5432
PGUSER=your_pg_user
PGPASSWORD=your_pg_password
PGDATABASE=your_pg_database

# Session Configuration
SESSION_SECRET=your_super_secret_session_key_here

# Development
NODE_ENV=development
```

### 4. Database Setup

Push the database schema:

```bash
npm run db:push
```

### 5. Populate Database (Optional)

If you have the provided dataset files, run the sentiment analysis population:

```bash
node populate-sentiment.js
```

## 🚀 Running the Application

### Development Mode

Start the development server:

```bash
npm run dev
```

The application will be available at:
- **Frontend & Backend**: http://localhost:5000

### Production Build

```bash
npm run build
npm start
```

## 📁 Project Structure

```
MarksandSpencers/
├── client/                 # Frontend React application
│   ├── src/
│   │   ├── components/     # Reusable UI components
│   │   ├── pages/         # Application pages
│   │   ├── hooks/         # Custom React hooks
│   │   └── lib/           # Utilities and helpers
├── server/                # Backend Express application
│   ├── auth.ts           # Authentication logic
│   ├── routes.ts         # API routes
│   ├── storage.ts        # Database operations
│   ├── db.ts             # Database connection
│   └── index.ts          # Server entry point
├── shared/               # Shared types and schemas
│   └── schema.ts         # Database schemas
├── attached_assets/      # Dataset files
└── package.json          # Dependencies and scripts
```

## 🔑 API Endpoints

### Authentication
- `POST /api/register` - User registration
- `POST /api/login` - User login
- `POST /api/logout` - User logout
- `GET /api/user` - Get current user

### Brands
- `GET /api/brands/:slug` - Get brand by slug
- `GET /api/brands` - List all brands

### Analytics
- `GET /api/brands/:id/metrics` - Social media metrics
- `GET /api/brands/:id/content` - Content performance
- `GET /api/brands/:id/demographics` - Audience demographics
- `GET /api/brands/:id/hashtags` - Trending hashtags
- `GET /api/brands/:id/topics` - Topic analysis
- `GET /api/brands/:id/sentiment` - Sentiment data

## 🎯 Usage

### 1. **User Registration**
- Create an account with email and password
- Complete profile information

### 2. **Dashboard Navigation**
- Use the sidebar to navigate between sections
- Select brand (Marks & Spencer or Next Retail)
- Choose platform (All, Instagram, TikTok, YouTube)
- Set date range (7 days, 30 days, 90 days)

### 3. **Analytics Features**
- **Overview Cards**: Key metrics at a glance
- **Mentions & Reach**: Platform comparison with toggle
- **Demographics**: Age and gender distribution
- **Content Performance**: Top performing posts
- **Trending Topics**: Real-time topic extraction

### 4. **Platform Filtering**
- Switch between platforms to see platform-specific data
- Use "Compare all platforms" toggle in charts
- Filter by date ranges for historical analysis

## 🗃️ Database Schema

The application uses the following main tables:

- **users** - User authentication and profiles
- **brands** - Brand information (Marks & Spencer, Next Retail)
- **social_metrics** - Platform metrics and engagement data
- **content_posts** - Individual social media posts
- **hashtags** - Trending hashtags and usage
- **audience_demographics** - Age and gender distribution
- **sentiment_data** - Sentiment analysis results

## 🔒 Authentication

The application uses session-based authentication with:
- Password hashing using scrypt
- PostgreSQL session storage
- Passport.js middleware
- Protected routes and API endpoints

## 📊 Data Sources

The platform processes authentic data from:
- **TikTok**: Hashtag scraper data
- **Instagram**: Hashtag and content data
- **YouTube**: Video and engagement metrics

## 🛠️ Development

### Available Scripts

```bash
npm run dev          # Start development server
npm run build        # Build for production
npm run db:push      # Push database schema
npm run type-check   # TypeScript type checking
```

### Code Structure

- **Frontend**: Modern React with TypeScript and Tailwind CSS
- **Backend**: Express.js with Drizzle ORM
- **Database**: PostgreSQL with authentic social media data
- **Authentication**: Session-based with Passport.js

## 🚀 Deployment

### Prerequisites for Deployment
1. PostgreSQL database (production)
2. Environment variables configured
3. Node.js hosting platform (Vercel, Railway, etc.)

### Steps
1. Set production environment variables
2. Run database migrations
3. Build the application
4. Deploy to your hosting platform

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is proprietary software for social media analytics.

## 📧 Contact

- **Developer**: Ananya
- **Repository**: https://github.com/Ananya786/MarksandSpencers
- **Issues**: https://github.com/Ananya786/MarksandSpencers/issues

## 🙏 Acknowledgments

- Authentic social media data from TikTok, Instagram, and YouTube platforms
- Modern React and Node.js ecosystem
- Tailwind CSS and Shadcn/ui for beautiful interfaces
- PostgreSQL for robust data management

---

**Built with ❤️ for comprehensive social media analytics**