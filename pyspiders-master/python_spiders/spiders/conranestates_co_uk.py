# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'conranestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = 'Conranestates_Co_PySpider_united_kingdom'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://conranestates.co.uk/property/?s=&cc_min-price=&cc_max-price=&cc_min-bedrooms=&cc_max-bedrooms=&cc_property-type=apartment&cc-status=for-rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://conranestates.co.uk/property/?s=&cc_min-price=&cc_max-price=&cc_min-bedrooms=&cc_max-bedrooms=&cc_property-type=house&cc-status=for-rent",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'ccp_list')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'information')]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type')}
            )
        
  
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//a[contains(@href,'reference')]/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('reference=')[1].strip())

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h3[contains(.,'About this property')]/following-sibling::p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//text()[contains(.,'sq ft')]").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('sq ft')[0].strip().split(' ')[-1].split('(')[-1].strip()) * 0.09290304)))

        room_count = response.xpath("//span[@class='bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bedroom')[0].strip())
        
        bathroom_count = response.xpath("//span[@class='bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].strip())

        rent = response.xpath("//h1/../span[@class='price']/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'available from')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('available from')[1].split('.')[0].strip().strip('early').strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x.split('url(')[1].split(')')[0]) for x in response.xpath("//div[contains(@class,'owl-stage')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//a[contains(.,'Floorplan')]/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        terrace = " ".join(response.xpath("//meta[@property='og:description']/@content").getall()).strip()
        if terrace:
            if 'terrace' in terrace:
                item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Conran Estates")
        item_loader.add_value("landlord_phone", "0800 689 3172")
        item_loader.add_value("landlord_email", "greenwich@conranestates.co.uk")

        yield item_loader.load_item()