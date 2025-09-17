CREATE VIEW AlbumsDetailed
AS

With Unified AS (
SELECT AlbumID, Title, ReleaseDate, BandName AS 'Artist'
FROM Albums A
JOIN Bands B ON A.BandID = B.BandID

UNION ALL

SELECT AlbumID, Title, ReleaseDate, CONCAT(FirstName, ' ', LastName) AS 'Artist'
FROM Albums A
JOIN Artists AR ON A.ArtistID = AR.ArtistID
)
SELECT Title, ReleaseDate, Unified.Artist
FROM Unified
ORDER BY AlbumID;

