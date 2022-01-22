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
    name = 'colomboimmobilier_fr' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source="Colomboimmobilier_PySpider_france"
    start_urls = ["https://colomboimmobilier.fr/"] #LEVEL-1

    # 1. FOLLOWING
    def start_requests(self):
        url =  "https://colomboimmobilier.fr/"
        yield Request(url,callback=self.parse)
 
    def parse(self,response):
        
        formdata = {
            "order": "DESC",
            "orderby": "id",
            "action": "rem_search_property",
            "property_purpose": "Location",
            "search_property": "",
            "property_city": "",
            "property_type": "",
            "price_min": "",
            "price_max": "",
        }
        headers={
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Mobile Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        yield FormRequest(
            "https://colomboimmobilier.fr/wp-admin/admin-ajax.php",
            formdata=formdata,headers=headers,
            callback= self.parse_list,
            dont_filter=True


        )
    def parse_list(self, response):
        for url in response.xpath("//div[@class='img-container']/a/@href").extract():
            yield Request(url,callback=self.populate_item)
            
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link",response.url)

        title=response.xpath("//div[@class='page_title']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//div[@class='page_title']/h1/text()").get()
        if property_type and "Duplex" in property_type:
            item_loader.add_value("property_type","house")
        description="".join(response.xpath("//div[@class='description']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//strong[.='Prix']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(".")[0].replace(",",""))
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//strong[.='Surface']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("M")[0].split(",")[0].strip())
        city=response.xpath("//strong[.='Région']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.split(":")[-1].strip())
        zipcode=response.xpath("//strong[.='Code postal']/following-sibling::text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(":")[-1].strip())
        adres=city.split(":")[-1].strip()+" "+zipcode.split(":")[-1].strip() 
        if adres:
            item_loader.add_value("address",adres)
        room_count=response.xpath("//strong[.='Pièces']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip())
        bathroom_count=response.xpath("//strong[.='Salles de bain']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1].strip())
        img=[]
        images=response.xpath("//div//img//@src").getall()
        if images:
            for i in images:
                if "Capture" in i:
                    img.append(i)
                    item_loader.add_value("images",img)
        item_loader.add_value("landlord_name","Colombo Immobilier")
        item_loader.add_value("landlord_phone"," 01 60 04 43 49")
        item_loader.add_value("landlord_email"," contact@colomboimmobilier.fr")

        yield item_loader.load_item()
