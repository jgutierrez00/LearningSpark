package Chapters.Chapter6

case class Bloggers
(
  id: BigInt,
  first: String,
  last: String,
  url: String,
  published: String,
  hits: BigInt,
  campaigns: Array[String]
)
