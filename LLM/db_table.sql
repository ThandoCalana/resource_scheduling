CREATE TABLE [dbo].[OutlookCalendarTest](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[user_email] [nvarchar](255) NULL,
	[first_name] [nvarchar](100) NULL,
	[date] [date] NULL,
	[time_slot] [time](7) NULL,
	[meeting_subject] [nvarchar](500) NULL,
	[start_time] [datetime] NULL,
	[end_time] [datetime] NULL,
	[load_percentage] [float] NULL,
	[content] [nvarchar](max) NULL)