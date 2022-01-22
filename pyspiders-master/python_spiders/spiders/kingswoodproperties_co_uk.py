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
    name = 'kingswoodproperties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = 'Kingswoodproperties_co_uk_PySpider_united_kingdom'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.kingswoodproperties.co.uk/?id=3077&do=search&for=2&type%5B%5D=8&minprice=0&maxprice=99999999999&minbeds=0&id=3077&order=2&page=0&do=search",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.kingswoodproperties.co.uk/?id=3077&do=search&for=2&type%5B%5D=24&minprice=0&maxprice=99999999999&minbeds=0&id=3077&order=2&page=0&do=search",
                    "https://www.kingswoodproperties.co.uk/?id=3077&do=search&for=2&type%5B%5D=22&minprice=0&maxprice=99999999999&minbeds=0&id=3077&order=2&page=0&do=search",
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
        page = response.meta.get('page', 1)
        seen=False

        for item in response.xpath("//div[@class='box-wrap featured-property results-list ']"):
            status = item.xpath(".//div[@class='featured-property-inner']/span/text()").get()
            if status and "to let" in status.lower():
                check_commercial = item.xpath(".//div[@class='col-sm-7 results-list-icons']/span[@class='catC2']/text()[contains(.,'Commercial')]").get()
                if check_commercial:
                    continue
                follow_url = item.xpath(".//div[@class='col-sm-5 property-pic']/a/@href").get()
                yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
                seen = True
        
        if page == 1 or seen:            
            p_url = response.url.replace(f"&page={page-1}&do=search", f"&page={page}&do=search")
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type": response.meta.get('property_type')})
                
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h2[@class='details-address1']/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("pid=")[-1].strip())

        rent = response.xpath("//p[@class='detail-price']/strong/text()").get()
        if rent:
            rent = rent.split(".")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//span[@class='icon-bed']/parent::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//span[@class='icon-bath']/parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lstrip().split(" ")[0])
        
        square_meters = response.xpath("//div[@class='value']/text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].split(".")[0].strip())
        
        address = response.xpath("//h2[@class='details-address1']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zip = address.split("| ")[-1]
            if zip:
                if zip:
                    item_loader.add_value("zipcode", zip)
            city = address.split("| ")[1].split(" |")[0].strip()
            if city:
                item_loader.add_value("city", city)
        
        features = "".join(response.xpath("//ul[@id='features_list']/li/text()").getall())
        if features and "parking" in features.lower():
            item_loader.add_value("parking", True)
        elif features and "garage" in features.lower():
            item_loader.add_value("parking", True)
        if features and "balcony" in features.lower():
            item_loader.add_value("balcony", True)
        if features and "furnish" in features.lower():
            item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[@id='details']/p/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='galleria']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
   
        item_loader.add_value("landlord_name", "Kingswood")
        item_loader.add_value("landlord_phone", "01772 71 71 81")
        item_loader.add_value("landlord_email", "mail@kingswoodproperties.co.uk")
        
        yield item_loader.load_item()