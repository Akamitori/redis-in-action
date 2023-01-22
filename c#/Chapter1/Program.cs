// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using StackExchange.Redis;

namespace Chapter1;

public class Program {
	private const int OneWeekInSeconds = 7 * 86400;
	private const int VoteScore = 432;
	private const int ArticlesPerPage = 25;
	private const string Upvoted = "upvoted:";
	private const string Downvoted = "downvoted:";

	enum VoteType {
		Upvote,
		Downvote
	}

	public static void Main() {
		new Program().run();
	}

	private void run() {
		var con = ConnectionMultiplexer.Connect("localhost");
		var db = con.GetDatabase();

		var articleId1 = postArticleAndDisplayData(db, "username", "A title", "https://www.google.com", VoteType.Upvote);
		var articleId2 = postArticleAndDisplayData(db, "some_other_guy", "A title", "https://www.google.com", VoteType.Upvote);

		Console.WriteLine();

		articleVote(db, "other_user", "article:" + articleId1, VoteType.Downvote);
		assertVotesForArticle(db, articleId1, 1, 0, 0);
		articleVote(db, "user_miracle", "article:" + articleId2, VoteType.Upvote);
		assertVotesForArticle(db, articleId1, 1, 0, 0);

		Console.WriteLine("The currently highest-scoring articles are:");
		var articles = getArticles(db, 1);
		printArticles(articles);
		Debug.Assert(articles.Count >= 1, "Article count is less than 1");

		addGroups(db, articleId1, new[] { "new-group" });
		addGroups(db, articleId2, new[] { "new-group" });
		Console.WriteLine("We added the article to a new group, other articles include:");
		var groupArticles = getGroupArticles(db, "new-group", 1);
		printArticles(groupArticles);
		Debug.Assert(groupArticles.Count >= 1, "Article group count is less than 1");
	}

	private string postArticleAndDisplayData(IDatabase db, string username, string title, string link, VoteType initialVoteType) {
		var articleId1 = postArticle(db, username, title, link, initialVoteType);
		Console.WriteLine("We posted a new article with id: " + articleId1);
		Console.WriteLine("Its HASH looks like:");
		var articleData1 = db.HashGetAll("article:" + articleId1);

		foreach (var entry in articleData1) {
			Console.WriteLine(" " + entry.Name + ": " + entry.Value);
		}

		return articleId1;
	}

	private static void assertVotesForArticle(IDatabase db, string articleId1, int votesLowerBound, int upvotesLowerBound, int downvotesLowerBound) {
		var votes = (int?)db.HashGet("article:" + articleId1, "votes") ?? 0;
		var upvotes = (int?)db.HashGet("article:" + articleId1, "upvotes") ?? 0;
		var downvotes = (int?)db.HashGet("article:" + articleId1, "downvotes") ?? 0;
		Console.WriteLine("We voted for the article, it now has votes: " + votes);
		Debug.Assert(votes > votesLowerBound, "Vote count is 1 or less");
		Debug.Assert(upvotes > upvotesLowerBound, "Upvote count is less than 1");
		Debug.Assert(downvotes > downvotesLowerBound, "Upvote count is less than 1");
	}

	private string postArticle(IDatabase db, string user, string title, string link, VoteType initialVoteType) {
		var articleId = db.StringIncrement("article:").ToString();

		var setToAddVote = initialVoteType == VoteType.Upvote ? Upvoted : Downvoted;
		db.SetAdd(setToAddVote, user);
		db.KeyExpire(setToAddVote, TimeSpan.FromSeconds(OneWeekInSeconds));

		var now = DateTimeOffset.Now.ToUnixTimeSeconds();
		var article = "article:" + articleId;
		var articleData = new List<HashEntry> {
			new("title", title),
			new("link", link),
			new("user", user),
			new("now", now.ToString()),
			new("votes", "1"),
			new("upvotes", initialVoteType == VoteType.Upvote ? "1" : "0"),
			new("downvotes", initialVoteType == VoteType.Downvote ? "1" : "0")
		};
		db.HashSet(article, articleData.ToArray());

		var modifier = initialVoteType == VoteType.Upvote ? 1 : -1;
		db.SortedSetAdd("score:", article, now + VoteScore * modifier);
		db.SortedSetAdd("time:", article, now);

		return articleId;
	}

	private void articleVote(IDatabase db, string user, string article, VoteType voteType) {
		var cutoff = DateTimeOffset.Now.ToUnixTimeSeconds() - OneWeekInSeconds;
		var articleScore = db.SortedSetScore("time:", article) ?? 0;

		if (articleScore < cutoff) {
			return;
		}

		var articleId = article.Substring(article.IndexOf(':') + 1);

		switch (voteType) {
			case VoteType.Upvote:
				if (!db.SetMove(Downvoted + articleId, Upvoted + articleId, user)) {
					if (db.SetAdd(Upvoted + articleId, user)) {
						db.SortedSetIncrement("score:", article, VoteScore);
						db.HashIncrement(article, "upvotes");
						db.HashIncrement(article, "votes");
					}
				} else {
					db.HashIncrement(article, "upvotes");
					db.HashDecrement(article, "downvotes");
					db.SortedSetIncrement("score:", article, VoteScore);
				}

				break;
			case VoteType.Downvote:
				if (!db.SetMove(Upvoted + articleId, Downvoted + articleId, user)) {
					if (db.SetAdd(Downvoted + articleId, user)) {
						db.SortedSetIncrement("score:", article, VoteScore);
						db.HashIncrement(article, "downvotes");
						db.HashIncrement(article, "votes");
					}
				} else {
					db.HashDecrement(article, "upvotes");
					db.HashIncrement(article, "downvotes");
					db.SortedSetDecrement("score:", article, VoteScore);
				}

				break;
			default:
				throw new ArgumentOutOfRangeException(nameof(voteType), voteType, null);
		}
	}

	private List<Dictionary<RedisValue, RedisValue>>
		getArticles(IDatabase db, int page, string order = "score:") {
		var start = (page - 1) * ArticlesPerPage;
		var end = start + ArticlesPerPage - 1;

		var ids = db.SortedSetRangeByRank(order, start, end, order: Order.Descending);
		var articles = new List<Dictionary<RedisValue, RedisValue>>();

		foreach (var id in ids) {
			var articleData = db.HashGetAll(id.ToString())
				.ToDictionary(c => c.Name, c => c.Value);
			articleData["id"] = id;
			articles.Add(articleData);
		}

		return articles;
	}

	private void printArticles(List<Dictionary<RedisValue, RedisValue>> articles) {
		foreach (var article in articles) {
			Console.WriteLine(" id: " + article["id"]);
			foreach (var articleData in article.Where(c => !c.Key.Equals("id"))) {
				Console.WriteLine("    " + articleData.Key + ": " + articleData.Value);
			}
		}
	}

	private void addGroups(IDatabase db, string articleId, string[] toAdd) {
		var article = "article:" + articleId;
		foreach (var group in toAdd) {
			db.SetAdd("group:" + group, article);
		}
	}

	private List<Dictionary<RedisValue, RedisValue>> getGroupArticles(IDatabase db, string group, int page, string order = "score:") {
		var key = order + group;
		if (!db.KeyExists(key)) {
			db.SortedSetCombineAndStore(SetOperation.Intersect, key, "group:" + group, order, aggregate: Aggregate.Max);
			db.KeyExpire(key, TimeSpan.FromSeconds(60));
		}

		return getArticles(db, page, key);
	}
}
