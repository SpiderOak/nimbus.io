/*
load cluster and node tables from diy schema
*/
delete from diy.cluster;
insert into diy.cluster (name, creation_time) values ('multi-node-cluster', current_timestamp);

delete from diy.node;
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 1, 'multi-node-01', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 2, 'multi-node-02', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 3, 'multi-node-03', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 4, 'multi-node-04', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 5, 'multi-node-05', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 6, 'multi-node-06', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 7, 'multi-node-07', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 8, 'multi-node-08', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 9, 'multi-node-09', 'localhost', current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from diy.cluster), 10, 'multi-node-10', 'localhost', current_timestamp);

