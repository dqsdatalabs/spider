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
    name = 'firstclasshousing_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = 'Firstclasshousing_PySpider_netherlands'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.firstclasshousing.nl/properties?page=1",
                ],
                "property_type": "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//li[@class='listing regular']"):
            status = item.xpath(".//p[@class='label']/text()").get()
            if status and "leased" in status.lower():
                continue
            
            base_url = "https://www.firstclasshousing.nl"
            follow_url = base_url + item.xpath(".//a/@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:            
            p_url = response.url.replace(f"?page={page-1}", f"?page={page}")
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type": response.meta.get('property_type')})
                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("normalize-space(//div[@class='left-column']/h1/text())").getall()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("-")[-1])
        
        rent = response.xpath("//span[@class='property-feature-label'][contains(.,'Price')]/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("€ ","").split(" ")[0].replace(",", ""))
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//span[@class='property-feature-label'][contains(.,'Bedroom')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        square_meters = response.xpath("//span[@class='property-feature-label'][contains(.,'area')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])

        available_date = response.xpath("//span[@class='property-feature-label'][contains(.,'Availability')]/following-sibling::span/text()").get()
        if available_date and ("per direct" not in available_date):
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        city = response.xpath("//span[@class='property-feature-label'][contains(.,'City')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address", city)
        
        latitude = response.xpath("//div[@id='mapview-canvas']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='mapview-canvas']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        furnished = response.xpath("//span[@class='property-feature-label'][contains(.,'Interior')]/following-sibling::span/text()").get()
        if furnished and "furnished" in furnished.lower():
            item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//p[@class='description left-column']/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
            if description and "terrace" in description.lower():
                item_loader.add_value("terrace", True)
            if description and "parking" in description.lower():
                item_loader.add_value("parking", True)
            if description and "dishwasher" in description.lower():
                item_loader.add_value("dishwasher", True)
            if description and "balcony" in description.lower():
                item_loader.add_value("balcony", True)
            if description and "elevator" in description.lower():
                item_loader.add_value("elevator", True)
        
        images = [response.urljoin(x) for x in response.xpath("//li/div/@data-image").getall()]
        if images:
            item_loader.add_value("images", images)  
        
        item_loader.add_value("landlord_name", "First Class Housing")
        item_loader.add_value("landlord_phone", "+31 20 811 08 11")
        item_loader.add_value("landlord_email", "info@firstclasshousing.nl")
        
        yield item_loader.load_item()