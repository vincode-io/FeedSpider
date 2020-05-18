WITH RECURSIVE subcategories AS (
	SELECT id, name, article_count, 0 depth
	FROM fs_category 
	WHERE name = 'crime'
	UNION
		SELECT c.id, c.name, c.article_count, sc.depth + 1
		FROM fs_category c
		INNER JOIN fs_category_relationship cr on c.id = cr.fs_category_child_id
		INNER JOIN subcategories sc ON sc.id = cr.fs_category_parent_id)
			SELECT * FROM subcategories
		
		
--select c2.name
--from fs_category c1
--inner join fs_category_relationship cr on c1.id = cr.fs_category_parent_id
--inner join fs_category c2 on cr.fs_category_child_id = c2.id
--where c1.name = 'main topic classifications'
--order by c2.name;