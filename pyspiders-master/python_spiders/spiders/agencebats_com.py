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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'agencebats_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Agencebats_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencebats.com/catalog/advanced_search_result.php?action=update_search&search_id=1718852613791122&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencebats.com/catalog/advanced_search_result.php?action=update_search&search_id=1718852613791122&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-product']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[contains(@class,'next-link')]/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        title = response.xpath("//div[@class='infos-products-header']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//div[@class='product-ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(": ")[1].strip())

        rent = response.xpath("//div[@class='product-price']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent = ''.join([n for n in rent if n.isdigit()])
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[@class='value']/text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])

        square_meters = response.xpath("//div[@class='value']/text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].split(".")[0].strip())
        
        address = response.xpath("//div[@class='product-localisation']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zip = address.split(" ")[0]
            if zip:
                if zip.isdigit():
                    item_loader.add_value("zipcode", zip)
            city = address.split(" ")[1]
            if city:
                if city and "saint" in city.lower():
                    city = "SAINT-ETIENNE"
                item_loader.add_value("city", city)
        
        terrace = response.xpath("//div[@class='value']/text()[contains(.,'terrain')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        desc = " ".join(response.xpath("//div[@class='product-description']/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
        
        item_loader.add_value("landlord_name", "Agence Bat's - ROCHE LA MOLIERE")
        item_loader.add_value("landlord_phone", "04.77.50.42.46")
        
        yield item_loader.load_item()