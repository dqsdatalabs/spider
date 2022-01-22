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
    name = 'agenziaghirardi_com'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Agenziaghirardi_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agenziaghirardi.com/ita/immobili?order_by=date_insert_desc&page=&rental=1&company_id=&seo=&luxury=&categories_id=&rental=1&typologies_multi%5B%5D=1&city_id=&range_size=&range_price=&code=&bedrooms_min=&bathrooms_min="
                ],      
                "property_type" : "apartment",
            }         
           
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='card']/a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        pagination = response.xpath("//li[@class='pager-pages current']//following-sibling::li[1]/a/@href").get()
        if pagination:
            follow_url = response.urljoin(pagination)
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        external_id = response.xpath("//span[@class='code']//text()").get()
        if external_id:
            external_id=external_id.split(".")[1]
            item_loader.add_value("external_id", external_id)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//div[@class='title-detail']/span[@class='type']//text()")

        address = "".join(response.xpath("//span[contains(@class,'location')]//text()").get())
        if address:
            item_loader.add_value("address",address)

            
        city = "".join(response.xpath("//span[contains(@class,'location')]//text()").get())
        if city:
            city=city.strip().split(",")[:1]
            item_loader.add_value("city",city)

        desc = "".join(response.xpath("//p[@class='description']//text()").getall())
        desc = desc.replace("\u00e8","").replace("\u20ac","").replace("\u201d","").replace("\u2019","").replace("\u2013","").replace("\u00c0","").replace("\u00e0","").replace("\r","").replace("\n","").replace("\t","")
        if desc:
            item_loader.add_value("description", desc)

        price = response.xpath("//p[@class='price']//text()").get()
        if price:
            item_loader.add_value("rent", price.split("€")[1])
        item_loader.add_value("currency", "EUR")

        dontallow=item_loader.get_output_value("description")
        if dontallow and "AFFITTATO!" in dontallow:
            return 

        square = response.xpath("//span[contains(.,'MQ')]//following-sibling::b//text()").get()
        if square:
            item_loader.add_value("square_meters", square)

        room_count = response.xpath("//span[contains(.,'Vani')]//following-sibling::b//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(".")[0])

        energy_label = response.xpath("//span[contains(.,'Classe Energ.')]//following-sibling::b//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        images = [response.urljoin(x)for x in response.xpath("//a[contains(@class,'swipebox')]//@href").extract()]
        if images:
                item_loader.add_value("images", images)
        
        terrace = response.xpath("//span[contains(.,'Terrazzo/i')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[@class='section']//h4[contains(.,'Caratteristiche Interne')]//following-sibling::ul//li//text()[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        balcony = response.xpath("//span[contains(.,'Balcone/i')]//following-sibling::b//text()").get()
        if balcony:
            if "0" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(');')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)  
            
        item_loader.add_value("landlord_phone", "+39 0106502067 ")
        item_loader.add_value("landlord_email", "info@agenziaghirardi.com")
        item_loader.add_value("landlord_name", "GHIRARDI AGENZIA IMMOBILIARE")

     

        yield item_loader.load_item()