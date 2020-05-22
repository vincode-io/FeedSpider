select * from fs_category where main_category = true and id not in (
select id from fs_category where name like '% %' and main_category = true
)
order by name