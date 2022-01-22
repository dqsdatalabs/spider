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
import dateparser
class MySpider(Spider):
    name = 'delamarcheimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Delamarcheimmobilier_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.delamarcheimmobilier.com/catalog/advanced_search_result.php?action=update_search&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_33_search=INFERIEUR&C_33_type=TEXT&C_33=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.delamarcheimmobilier.com/catalog/advanced_search_result.php?action=update_search&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_33_search=INFERIEUR&C_33_type=TEXT&C_33=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&keywords=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            dont_filter=True,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='product-listing']"):
            follow_url = response.urljoin(item.xpath("./a//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        # next_page = response.xpath("//a[@class='suivant']/@href").get()
        # if next_page:
        #     p_url = response.urljoin(next_page)
        #     yield Request(
        #         p_url,
        #         callback=self.parse,
        #         meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", self.external_source)
        title =" ".join(response.xpath("//meta[@property='og:title']/@content").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))        
         
        address = response.xpath("//div[@class='contain_nav']//h2/span/text()").extract_first()
        if address:
            address = address.replace(" - ","").strip()
            item_loader.add_value("address",address )   
            item_loader.add_value("city",address) 
        zipcode = response.xpath("//span[@class='alur_location_ville']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[0] )   

        item_loader.add_xpath("external_id", "substring-after(//span[@itemprop='name']/text()[contains(.,'Ref')],': ')")
        item_loader.add_xpath("room_count", "//li[contains(@class,'list-group-item')]/div[div[.='Nombre pièces']]/div[2]//text()")
        item_loader.add_xpath("bathroom_count", "//li[contains(@class,'list-group-item')]/div[contains(.,'Salle')]//div//b//text()")
        rent =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Loyer mensuel')]//div//b//text()").extract_first()
        if rent:     
            item_loader.add_value("rent",rent.replace('\EUR', '').replace(' ',''))  
        item_loader.add_value("currency", "EUR")
        
        utilities =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Provision sur')]//div//b//text()").extract_first()
        if utilities:     
            utilities = utilities.split("EUR")[0].strip().replace('\xa0', '').replace(' ','')
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
   
        deposit =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Dépôt de')]//div//b//text()").extract_first()
        if deposit:   
            deposit = deposit.split("EUR")[0].strip().replace('\xa0', '').replace(' ','')
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
        
        available_date = response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'de disponibilité')]//div//b//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        square =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Surface')]//div//b//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        energy_label =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Diagnostic ')]//div//b//text()").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  
   
        item_loader.add_xpath("floor","//li[@class='list-group-item']/div[div[.='Nombre étages']]/div[2]//text()")  
        parking =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'garages')]//div//b//text()").extract_first()    
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        swimming_pool =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Piscine')]//div//b//text()").extract_first()    
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
 
        elevator =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Ascenseur')]//div//b//text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        furnished =response.xpath("//li[contains(@class,'list-group-item')]/div[contains(.,'Meublé')]//div//b//text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
    
        desc = " ".join(response.xpath("//div[@class='product-description']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']//a//@href").extract()]
        if images:
                item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'= new google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("= new google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        
        landlord_name = response.xpath("//div[@class='name-agence']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("//div[@class='tel-agence']//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()