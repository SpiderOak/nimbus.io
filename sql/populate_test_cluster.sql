/*
load cluster and node tables from diy schema
*/
delete from diy_central.cluster;
insert into diy_central.cluster (name) values ('multi-node-cluster');

delete from diy_central.node;
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 1, 'multi-node-01', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 2, 'multi-node-02', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 3, 'multi-node-03', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 4, 'multi-node-04', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 5, 'multi-node-05', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 6, 'multi-node-06', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 7, 'multi-node-07', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 8, 'multi-node-08', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 9, 'multi-node-09', 'localhost');
insert into diy_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from diy_central.cluster), 10, 'multi-node-10', 'localhost');

