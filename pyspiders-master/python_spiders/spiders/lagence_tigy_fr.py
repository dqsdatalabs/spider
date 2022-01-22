# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'lagence_tigy_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lagence-tigy.fr/catalog/advanced_search_result.php?action=update_search&search_id=1689488559998671&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lagence-tigy.fr/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-product']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[contains(@class,'next-link')]/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Lagence_Tigy_PySpider_france")
        item_loader.add_xpath("title", "//h1/text()")        
         
        address =response.xpath("//div[@class='product-localisation']//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip()) 
            zipcode = address.strip().split(" ")[0]
            city = " ".join(address.strip().split(" ")[1:])
            item_loader.add_value("zipcode",zipcode.strip()) 
            item_loader.add_value("city",city.strip()) 
       
        external_id = response.xpath("substring-after(//div[@class='product-ref']//text(),':')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
     
        room_count =response.xpath("//ul[@class='list-criteres']//div[contains(.,'chambre')]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.split("chambre")[0].strip()) 
        else:
            room_count =response.xpath("//ul[@class='list-criteres']//div[contains(.,'pièce')]/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.split("pièce")[0].strip()) 
        
        bathroom_count =response.xpath("//ul[@class='list-criteres']//div[contains(.,'salle(s) d')]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("salle")[0].strip()) 
 
        rent =" ".join(response.xpath("//div[@class='prix loyer']//text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace(' ','').replace("\xa0","."))  
       
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").extract_first()
        if utilities:   
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        deposit = response.xpath("//span[@class='alur_location_depot']/text()").extract_first()
        if deposit:    
            deposit = deposit.split(":")[-1].split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
  
        square =response.xpath("//ul[@class='list-criteres']//div[contains(.,'m²')]/text()[not(contains(.,'terrain'))]").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        desc = " ".join(response.xpath("//div[@class='product-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "un garage" in desc.lower():
                item_loader.add_value("parking", True)  
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']/div/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        latlng = response.xpath("//script/text()[contains(.,'myLatlng = new google.maps.LatLng(')]").get()
        if latlng:
            latlng = latlng.split("myLatlng = new google.maps.LatLng(")[1].split(");")[0].strip()
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
     
        item_loader.add_value("landlord_name", "L'AGENCE TIGY")
        item_loader.add_value("landlord_phone", "02.38.58.10.79")

        yield item_loader.load_item()