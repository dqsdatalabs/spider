# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import dateparser

class MySpider(Spider):
    name = 'relianceresidential_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.zoopla.co.uk/to-rent/branch/reliance-residential-cheshunt-77212/?branch_id=77212&include_rented=true&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine&page_size=100",
                "property_type" : "house"
            },
            {
                "url" : "https://www.zoopla.co.uk/to-rent/branch/reliance-residential-cheshunt-77212/?branch_id=77212&include_rented=true&include_shared_accommodation=false&page_size=100&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'listing-results-price')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
           
        
        
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
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        
        item_loader.add_value("external_source", "Relianceresidential_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div//h1//text()").get()
        item_loader.add_value("title", title)
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
        
        json_value = "".join(response.xpath("//script[@type='application/ld+json']//text()").extract())
        if json_value:
            json_l = json.loads(json_value)
            for j in json_l["@graph"]:
                if j.get("geo"):
                    lat = j.get("geo").get("latitude")
                    lng = j.get("geo").get("longitude")
                    if lat and lng:
                        item_loader.add_value("longitude", str(lng))
                        item_loader.add_value("latitude", str(lat))
                if j.get("photo"):
                    images = []
                    for img in j.get("photo"):
                        img_url = img.get("contentUrl")
                        images.append(img_url)
                    item_loader.add_value("images", images)
        
        if "terrace" in title.lower():
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "Reliance Residential")
        item_loader.add_value("landlord_phone", "01992 843787")
        
        yield item_loader.load_item()