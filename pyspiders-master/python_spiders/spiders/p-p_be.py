# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'p-p_be'
    execution_type = 'testing' 
    country = 'belgium'
    locale='nl'
    external_source='Pp_PySpider_belgium'
    custom_settings = {"HTTPCACHE_ENABLED": False}
    def start_requests(self):

        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9,tr;q=0.8",
            "content-type": "application/json"
        }
        payload = {"Transaction":"2","Type":"0","City":"0","MinPrice":"0","MaxPrice":"0","MinSurface":"0","MaxSurface":"0","MinSurfaceGround":"0","MaxSurfaceGround":"0","MinBedrooms":"0","MaxBedrooms":"0","Radius":"0","NumResults":"15","StartIndex":1,"ExtraSQL":"0","ExtraSQLFilters":"0","NavigationItem":"0","PageName":"0","Language":"NL","CountryInclude":"0","CountryExclude":"0","Token":"UWZBRYBQNTVDWWTKQNVEXATXEITGIXOAYNFHJBEYKSIEWXGSGZ","SortField":"1","OrderBy":1,"UsePriceClass":False,"PriceClass":"0","SliderItem":"0","SliderStep":"0","CompanyID":"0","SQLType":"3","MediaID":"0","PropertyName":"0","PropertyID":"0","ShowProjects":False,"Region":"0","currentPage":"0","homeSearch":"0","officeID":"0","menuIDUmbraco":"0","investment":False,"useCheckBoxes":False,"CheckedTypes":"0","newbuilding":False,"bedrooms":0,"latitude":"0","longitude":"0","ShowChildrenInsteadOfProject":False,"state":"0","FilterOutTypes":""}
        yield Request(
            "https://p-p.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0",
            callback=self.parse,
            body=json.dumps(payload),
            method="POST",
            headers=headers,
            dont_filter=True
        )
    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)
        for item in data:
            type=str(item['Property_SEO']).strip().split(" ")[0].lower()
            id=str(item['Property_URL']).split("/0/")[0]
            country=str(item['Property_SEO']).strip().split(" ")[-1].lower()
            url=f"https://p-p.be/nl/te-huur/{country}/{type}/{id.split('/')[-1]}/"
            yield Request(url, callback=self.populate_item,meta={'property_type':type,'item':item})
        

    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item=response.meta.get("item")
        print(item)
        print("***")
        external_id=item['FortissimmoID']
        if external_id:
            item_loader.add_value("external_id",external_id)
        city=item['Property_City_Value']
        if city:
            item_loader.add_value("city",city)
        description=item['Property_Description']
        if description:
            item_loader.add_value("description",description)
        latitude=item['Property_Lat']
        if latitude:
            item_loader.add_value("latitude",str(latitude))
        longitude=item['Property_Lon']
        if longitude:
            item_loader.add_value("longitude",str(longitude))
        title=item['Property_Title']
        if title:
            item_loader.add_value("title",title)
        zipcode=item['Property_Zip']
        if title:
            item_loader.add_value("zipcode",zipcode)
        room_count=item['bedrooms']
        if room_count:
            item_loader.add_value("room_count",room_count)
        property_type=response.meta.get('property_type')
        if property_type=="appartement":
            item_loader.add_value("property_type","apartment")
        property_type=response.meta.get('property_type')
        if property_type=="woning":
            item_loader.add_value("property_type","house")
        rent=response.xpath("//label[contains(.,'Huurprijs:')]/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        img=[]
        images=response.xpath("//script[contains(.,'Images')]/text()").get()
        if images:
            images=images.split("src: '")
            for i in images:
                img.append(i.split("'});")[0])
                item_loader.add_value("images",img)
        adres=response.xpath("//div[@class='shortinfo']/h3/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace("\r","").replace("\n","").strip())
        square_meters=response.xpath("//label[.='Bewoonbare opp.:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0].strip())
        item_loader.add_value("landlord_name","Petit Petre")
        phone=response.xpath("//i[@class='fa fa-mobile']/following-sibling::a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

            
        
    


        yield item_loader.load_item()