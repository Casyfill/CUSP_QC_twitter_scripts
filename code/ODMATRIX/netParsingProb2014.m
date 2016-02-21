function netParsingProb2014(custtype,networktype,normalization)
  %create BBVA and non-BBVA probabilistic 368area2area mobility networks
  %per demography for BBVA and per country of origin for non-BBVA
  %custtype=1 (BBVA), 2(non-BBVA)
  %networktype=1 (colocation), 2(probabilistic)
  
  %normalization=1;
  if normalization
    prefix='_norm';  
  else
    prefix='';  
  end
  
  switch custtype
      case 1
          if networktype==2
            netfilesave=strcat('netParsing2014/BBVAprobnetwork368',prefix,'.mat');
          else
            netfilesave=strcat('netParsing2014/BBVAcolocnetwork368',prefix,'.mat');  
          end
          custmobilityfile='netParsing2014/BBVAcustmobility368.mat';
      case 2
          if networktype==2
            netfilesave=strcat('netParsing2014/NONBBVAprobnetwork368',prefix,'.mat');
          else
            netfilesave=strcat('netParsing2014/NONBBVAcolocnetwork368',prefix,'.mat');  
          end
          custmobilityfile='netParsing2014/NONBBVAcustmobility368.mat';
  end
  
  %%%% create customer mobility data
  if ~exist(custmobilityfile,'file')
      load('networks/bistable_wareas.mat','bistable'); 
      pos=[bistable(:,1) bistable(:,10)];
      cd('/Users/stansobolevsky/Desktop/MY_WORKSPACE/MIT5/BBVAparsing/data'); 
      custmobility=dataAggregateOnTimeWindow(custtype==1, '110101', '000000', '111231', '235959', pos, []);
      cd('/Users/stansobolevsky/Desktop/MY_WORKSPACE/MIT5/BBVA3');
      save(custmobilityfile,'custmobility');
      clear custmobility;
  end
  
  fprintf('Define customer categories...\n');
  switch custtype
      case 1
          load('/Users/stansobolevsky/Desktop/MY_WORKSPACE/MIT5/BBVAparsing/data/customer_demography.mat');
          gender=1*(customer_demography(:,2)=='V')+2*(customer_demography(:,2)=='M');
          age=2011-customer_demography(:,3);
          custdupl=sparse(customer_demography(:,1),1,1); %check for duplicated records
          custind=(age>=15)&(age<100)&(gender>0)&full(custdupl(customer_demography(:,1))==1); %index of valid records
          custcat=sparse(customer_demography(custind,1),1,gender(custind)*100+age(custind));
      case 2
          load('/Users/stansobolevsky/Desktop/MY_WORKSPACE/MIT5/BBVAparsing/data/nonBBVA_customer.mat','nonBBVA_customer');
          custcat=sparse(nonBBVA_customer(:,1),1,nonBBVA_customer(:,2));
  end
  
  fprintf('Filtering the customers...\n');
  load(custmobilityfile,'custmobility');
  
  custmobility=custmobility(custmobility(:,1)>0,:);
  
  custw=[];
  if normalization&&(custtype==1)
      provmarketshare=csvread('data/prov_marketshare.csv');
      load('/Users/stansobolevsky/Desktop/MY_WORKSPACE/MIT5/BBVAparsing/data/customer_location.mat','customer_location');
      cust_share=fix(customer_location(:,2)/1000);
      cust_share=(provmarketshare(cust_share+(cust_share==0)).^(-1)).*(cust_share>0);
      cust_share=sparse(customer_location(:,1),1,cust_share,max([customer_location(:,1); custmobility(:,2)]),1);
      if networktype==2
        custmobility(:,3)=custmobility(:,3).*full(cust_share(custmobility(:,2)));
      else
        custw=cust_share;  
      end
  end
  
  if normalization&&(custtype==2)&&(networktype==2)
      if ~exist('data/zip2area368.mat','file')
          shapes=shaperead('Spainmap/ESP_adm/ESP_adm3.shp');
          load('data/zipgeo_table.mat','table');
          table=table(table(:,1)>0,:);
          table(end,4)=0;
          for a=1:length(shapes)
            table(inpolygon(table(:,3),table(:,2),shapes(a).X,shapes(a).Y),4)=a;  
          end
          zip2area=table(:,[1 4]);
          save('data/zip2area368.mat','zip2area');
      else
          load('data/zip2area368.mat','zip2area');
      end
      
      load('data/zip_marketshare.mat','zip_all','zip_bbvaloc');
      %zip2area=csvread('data/zip2adm3.csv');
      zip2area=zip2area(zip2area(:,2)>0,:);
      area_all=accumarray(zip2area(:,2),full(zip_all(zip2area(:,1))));
      area_bbvaloc=accumarray(zip2area(:,2),full(zip_bbvaloc(zip2area(:,1))));
      custmobility(:,3)=custmobility(:,3)./area_bbvaloc(custmobility(:,1)).*area_all(custmobility(:,1));
  end
  
  custactivity=sparse(custmobility(:,2),1,custmobility(:,3));
  ind=full(custactivity(custmobility(:,2)));
  ind=(ind>=(2+10*(custtype==1)))&(ind<=3650)&(custmobility(:,2)<=size(custcat,1));
  custmobility=custmobility(ind,:);
  custmobility=custmobility(custcat(custmobility(:,2))>0,:);
  custcat_=full(custcat(custmobility(:,2)));
  
  %condence customer categories
  [catmap,~,custcat_]=unique(custcat_);
  
  catactivity=accumarray(custcat_,custmobility(:,3));
  custactivity=sparse(custmobility(:,2),1,custmobility(:,3));
  
  %if 0
  
  fprintf('Creating the network...\n');
  
  catnum=length(catmap); areanum=368;
  network=zeros(areanum,areanum,catnum);
  custtotal_=full(custactivity(custmobility(:,2)));
  
  for a=1:areanum
      fprintf('Processing area %d...\n',a);
      custa=sparse(custmobility(:,2),1,custmobility(:,3).*(custmobility(:,1)==a));
      custa_=full(custa(custmobility(:,2)));
      if isempty(custw)
        custw_=1;  
      else
        custw_=full(custw(custmobility(:,2)));
      end
      if networktype==2
        network(a,:,:)=accumarray([custmobility(:,1) custcat_],custa_./(custtotal_-1).*(custmobility(:,3)-(custmobility(:,1)==a)),[areanum catnum]);
      else
        network(a,:,:)=accumarray([custmobility(:,1) custcat_],custw_.*(custa_>0).*(custmobility(:,3)>(custmobility(:,1)==a)),[areanum catnum]);  
      end
  end
  save(netfilesave,'network','catmap','catactivity');
  %else
  %  load(netfilesave,'network','catmap');  
  %  save(netfilesave,'network','catmap','catactivity');
  %end
  
 
  
end