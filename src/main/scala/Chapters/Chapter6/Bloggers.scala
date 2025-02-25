package Chapters.Chapter6

case class Bloggers
(
  id: Int,
  first: String,
  last: String,
  url: String,
  date: String,
  hits: Int,
  campaigns: Array[String]
)
