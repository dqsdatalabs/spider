# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json 
from  geopy.geocoders import Nominatim 
from html.parser import HTMLParser
import math 
from scrapy.selector import Selector

class MySpider(Spider):
    name = 'dg-realestate_com' 
    execution_type='testing'
    country='belgium'
    locale='fr' # LEVEL 1
    external_source="DgRealestate_PySpider_belgium"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://dg-realestate.com/ajax/submit/woonvastgoed/SearchType?page=1&amountPerPage=24&status=te-huur&type=appartement"],
                "property_type": "apartment"
            },

        ] # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,callback=self.parse,meta={"property_type":url.get('property_type')})
    def parse(self, response):
        data=json.loads(response.body)
        for item in data:
            name=str(item['name']).lower()
            reference=item['reference']
            follow_url = f"https://dg-realestate.com/aanbod/woonvastgoed/detail/{reference}/{name}"
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("property_type",response.meta.get("property_type"))

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=title
        if adres:
            item_loader.add_value("address",adres.split("|")[1].split("|")[0])
        price=response.xpath("//img[contains(@src,'price')]/following-sibling::span/text()").get()
        if price:
            item_loader.add_value("rent",price.split("€")[1].split("/ma")[0].strip())
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//img[contains(@src,'area-detail')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        room_count=response.xpath("//img[contains(@src,'bed')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//img[contains(@src,'bath')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        description="".join(response.xpath("//h2[.='Beschrijving']//following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        external_id=response.xpath("//div[contains(.,'Referentie')]/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\n","").strip())
        terrace=response.xpath("//div[contains(.,'Terras')]/following-sibling::div/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        latitude=response.xpath("//div[@id='latitude']/@data-item").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//div[@id='longitude']/@data-item").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        images=[x for x in response.xpath("//a[@data-fancybox='detail']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Jarno Vandermeren")
        item_loader.add_value("landlord_phone","03 283 43 33")
        item_loader.add_value("landlord_email","jv@dg-realestate.com")
        yield item_loader.load_item()
