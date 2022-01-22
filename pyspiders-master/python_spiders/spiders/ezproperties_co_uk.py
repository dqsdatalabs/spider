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
    name = 'ezproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.zoopla.co.uk/to-rent/branch/ez-property-services-ltd-middlesex-117309/?branch_id=117309&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.zoopla.co.uk/to-rent/branch/ez-property-services-ltd-middlesex-117309/?branch_id=117309&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')}
        )
            
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'listing-results')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        external_id="".join(response.url)
        if external_id:
            external_id="".join(external_id.split("/")[-2:-1])
            item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_source", "Ezproperties_Co_PySpider_united_kingdom")
        script = response.xpath("//script[contains(.,'@type')]/text()").get()
        description = ""
        title = ""
        if script:
            data = json.loads(script)["@graph"][3]
            
            title = data["name"]
            item_loader.add_value("title", title)
            
            item_loader.add_value("description", data["name"])
            
            address = data["address"]["streetAddress"]
            district = data["address"]["addressLocality"]
            city = data["address"]["addressRegion"]
            item_loader.add_value("address", f"{address} {district} {city}")
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", address.split(" ")[-1])
            
            latitude = data["geo"]["latitude"]
            longitude = data["geo"]["longitude"]
            item_loader.add_value("latitude", str(latitude))
            item_loader.add_value("longitude", str(longitude))
            
            for i in data["photo"]:
                item_loader.add_value("images", i["contentUrl"])

        if title and "studio" in title.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
        
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
        
        item_loader.add_value("landlord_name", "EZ Property Services Ltd")
        item_loader.add_value("landlord_phone", "020 3641 6760")
        
        yield item_loader.load_item()