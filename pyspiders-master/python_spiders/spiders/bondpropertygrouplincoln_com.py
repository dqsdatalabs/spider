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

class MySpider(Spider):
    name = 'bondpropertygrouplincoln_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Bondpropertygrouplincoln_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.zoopla.co.uk/to-rent/branch/bond-and-co-lincoln-86705/?branch_id=86705&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=highest_price&search_source=refine", "property_type": "apartment"},
	        {"url": "https://www.zoopla.co.uk/to-rent/branch/bond-and-co-lincoln-86705/?branch_id=86705&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=highest_price&search_source=refine", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        prop_type = response.meta.get("property_type")

        for item in response.xpath("//ul[contains(@class,'listing-results')]/li//a[contains(@class,'listing-results-price')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop_type})
        
        pagination = response.xpath("//div[contains(@class,'paginate')]/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse,meta={"property_type":prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = "".join(response.xpath("//li[@class='ui-property-indicators__item']/span/text()").extract())
        if "let" in rented.lower():
            return

        item_loader.add_xpath("title", "//h1[contains(@class,'ui-property-summary__title')]/text()")
        prop_type = response.xpath("//h1[contains(@class,'ui-property-summary__title')]/text()").get()
        if prop_type and "Studio" in prop_type :
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Bondpropertygrouplincoln_PySpider_united_kingdom")

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])

        script = response.xpath("//script[contains(.,'@type')]/text()").get()
        description = ""
        if script:
            script = script.split('"@graph":')[1].strip().split('"name": "')[-1]
            title = script.split('"')[0].strip()
            item_loader.add_value("title", title)
            
            description = script.split('description": "')[1].split('"')[0]
            item_loader.add_value("description", description)
            
            address = script.split('streetAddress": "')[1].split('"')[0]
            district = script.split('addressLocality": "')[1].split('"')[0]
            city = script.split('addressRegion": "')[1].split('"')[0]
            item_loader.add_value("address", f"{address} {district} {city}")
            item_loader.add_value("city", city)
            
            latitude = script.split('latitude":')[1].split(',')[0]
            longitude = script.split('longitude":')[1].split('}')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
            image = script.split('photo":')[1].split("]")[0]+"]"
            images = json.loads(image)
            for i in images:
                item_loader.add_value("images", i["contentUrl"])
        
        rent = response.xpath("//span[@data-testid='price']//text()").get()
        if rent:
            rent = rent.split(" ")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//span[@data-testid='beds-label']//text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            item_loader.add_value("room_count", room_count)
        elif "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//span[@data-testid='baths-label']//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        item_loader.add_value("landlord_name", "Bond & Co")
        item_loader.add_value("landlord_phone", "01522 397852")

        yield item_loader.load_item()