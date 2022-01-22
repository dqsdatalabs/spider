# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request 
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re  
import dateparser  

class MySpider(Spider):
    name = "gestea_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='nl'
    external_source='Gestea_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.gestea.be/en/estates/?state=torent&estate_type=537&price_min=0&price_max=10000&estate_furnished=0&estate_rooms=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.gestea.be/en/estates/?state=torent&estate_type=521&price_min=0&price_max=10000&estate_furnished=0&estate_rooms=",
                "property_type" : "house"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type')})
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='estate_card']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            next_page = f"https://www.gestea.be/en/estates/page/{page}/?state=torent&estate_type=537&price_min=0&price_max=10000&estate_furnished=0&estate_rooms#038;estate_type=537&price_min=0&price_max=10000&estate_furnished=0&estate_rooms"
            if next_page:        
                yield Request(next_page,callback=self.parse,meta={'property_type': response.meta.get('property_type'),'page':page+1})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//div[@class='small-12 medium-8 columns']//h1/text()").get())
        if adres:
            item_loader.add_value("address",adres)
        description="".join(response.xpath("//div[@class='medium-5 small-12 columns']/div//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a[@data-fancybox='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//div[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ","").strip())
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//div[contains(.,'Rooms ')]/following-sibling::div/text()").get()
        if room_count: 
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[contains(.,'Salles de bain')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        furnished=response.xpath("//div[contains(.,'Furnished')]/following-sibling::div/text()").get()
        if furnished and "Yes" in furnished:
            item_loader.add_value("furnished",True)
        floor=response.xpath("//div[contains(.,'Number of floors')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        square_meters=response.xpath("//div[contains(.,'Living area')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("new google.maps.Marker({")[-1].split("map: map")[0].split("lat:")[-1].split(",")[0])
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("new google.maps.Marker({")[-1].split("map: map")[0].split("lng:")[-1].split("}")[0])
        
        item_loader.add_value("landlord_name","GESTEA Real Estate Solution")
        email=item_loader.get_output_value("description")
        if email:
            email=email.split("INFO & VISITS")[-1]
            emails=re.findall(r'[\w\.-]+@[\w\.-]+',email)
            item_loader.add_value("landlord_email",emails)
        phone=item_loader.get_output_value("description")
        if phone:
            phone=re.findall(r'\d{4}\s\d{2}\s\d{2}\s\d{2}',phone)
            if phone:
                item_loader.add_value("landlord_phone",phone)
        yield item_loader.load_item()