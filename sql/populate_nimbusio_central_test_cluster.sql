/*
load cluster and node tables from nimbusio_central schema
*/

/* XXX: why is this not a transaciton? 
should we drop the multi-node notation? nimbus-node? everything is a multi-node
here.
*/

delete from nimbusio_central.cluster;
insert into nimbusio_central.cluster (name) values ('multi-node-cluster');

delete from nimbusio_central.node;
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 1, 'multi-node-01', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 2, 'multi-node-02', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 3, 'multi-node-03', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 4, 'multi-node-04', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 5, 'multi-node-05', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 6, 'multi-node-06', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 7, 'multi-node-07', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 8, 'multi-node-08', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 9, 'multi-node-09', 'localhost');
insert into nimbusio_central.node (cluster_id, node_number_in_cluster, name, hostname)
values ((select id from nimbusio_central.cluster), 10, 'multi-node-10', 'localhost');

