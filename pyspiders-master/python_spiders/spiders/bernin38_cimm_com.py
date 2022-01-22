# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math


class MySpider(Spider):
    name = 'bernin38_cimm_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://cimm.com/location/maison/BERNIN", "property_type": "house"},
            {"url": "https://cimm.com/location/appartement/BERNIN", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='ember-view']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = "".join(response.xpath("//div[@class='detail']/h3//text()").extract())
        item_loader.add_value("title", title.strip())
        item_loader.add_value("property_type", response.meta.get('property_type'))


        item_loader.add_value("external_source", "Bernin38_cimm_com_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        rent =  ".".join(response.xpath("//div[@class='price']/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        external_id =  "".join(response.xpath("//div[@class='detail']/h4/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])

        meters = response.xpath("//ul[@class='caracteristiques']/li/i[ @class='fa fa-arrows-alt']/following-sibling::text()[1]").extract_first()
        if meters:
            s_meters = meters.replace("m","")
            item_loader.add_value("square_meters", math.ceil(float(s_meters)))

        room_count =  "".join(response.xpath("//ul[@class='caracteristiques']/li/i[ @class='fa fa-bed']/following-sibling::text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0].strip())

        address =  ".".join(response.xpath("//div[@class='detail']/h3[2]//text()").extract())
        if address:
            item_loader.add_value("address", address.split(",")[-1])
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])

        desc = "".join(response.xpath("//p[@class='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        # deposit = "".join(response.xpath("//div[@class='price_plus']/p[contains(.,'garantie')]//text()").getall())
        # if deposit:
        #     deposit = deposit.split(":")[1].split("€")[0].strip()
        #     item_loader.add_value("deposit", deposit)
        if "de garantie :" in desc:
            deposit =  desc.split("de garantie :")[1].split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//span[contains(@class,'honoraire')]//text()").getall())
        if utilities:
            utilities = utilities.split("€")[0].strip().split("dont")[1].strip()
            if utilities != "0":
                item_loader.add_value("utilities", utilities)
        
        images = [x for x in response.xpath("//meta[@name='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_phone", "0603790607")
            
        item_loader.add_value("landlord_name", "Cimm Immobilier Bernin 38")
        yield item_loader.load_item()