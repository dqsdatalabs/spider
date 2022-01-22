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
class MySpider(Spider):
    name = 'budgethousing_nl' 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://budgethousing.nl/zoekresultaten/?filter_search_action%5B%5D=huur&adv6_search_tab=huur&term_id=134&term_counter=0&advanced_city=&filter_search_type%5B%5D=appartement&aantal-kamers=&aantal-slaapkamers=&min-oppervlakte=&price_low_134=0&price_max_134=5000&submit=Zoeken+naar+woningen", 
                ],
                "property_type": "apartment"
                },
	        {
                "url": [
                    "https://budgethousing.nl/zoekresultaten/?filter_search_action%5B%5D=huur&adv6_search_tab=huur&term_id=134&term_counter=0&advanced_city=all&filter_search_type%5B%5D=woning&aantal-kamers=&aantal-slaapkamers=&min-oppervlakte=&price_low_134=0&price_max_134=5000&submit=Search+for+homes", 
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://budgethousing.nl/zoekresultaten/?filter_search_action%5B%5D=huur&adv6_search_tab=huur&term_id=134&term_counter=0&advanced_city=all&filter_search_type%5B%5D=studio&aantal-kamers=&aantal-slaapkamers=&min-oppervlakte=&price_low_134=0&price_max_134=5000&submit=Zoeken+naar+woningen", 
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'link')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
        
    
#     # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        rented = response.xpath("//p[@class='status']//text()[contains(.,'Verhuurd') or contains(.,'Verkocht')]").extract_first()
        if rented:
            return
        item_loader.add_value("external_source", "Budgethousing_PySpider_netherlands")

        title = response.xpath("//h1[contains(@class,'entry-title')]//text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip())  
            # if "gemeubileer" in title.lower():
            #     item_loader.add_value("furnished", True )
 
        # ext_id = response.xpath("//div[@class='rh_property__id']/p[2]//text()").extract_first()
        # if ext_id:
        #     item_loader.add_value("external_id", ext_id.strip())

        address = response.xpath("//div[@class='property_categs']/text()[2]").get()
        city = response.xpath("//div[contains(@class,'property_categs')]//a//text()").get()
        if address:
            item_loader.add_value("address", f"{address} {city}".strip())
        
        if city:
            item_loader.add_value("city", city) 

        room_count = response.xpath("//strong[contains(.,'slaapkamers')]//parent::div/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//strong[contains(.,'kamers:')][not(contains(.,'slaap') or contains(.,'bad'))]//parent::div/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//strong[contains(.,'badkamers')]//parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[contains(@class,'price_area')]/text()").extract_first()
        if rent:    
            rent = rent.replace("€","").strip().replace(".","")
            item_loader.add_value("rent", rent)     
        item_loader.add_value("currency", "EUR")
        
        deposit =response.xpath("//div[contains(@class,'description')]//text()[contains(.,'Borgsom')]").extract_first()
        if deposit and "€" in deposit:
            deposit = deposit.split("€")[1].replace(".","").replace("–","").replace("-","").replace(",","").strip()
            item_loader.add_value("deposit", deposit)

        square =  response.xpath("//strong[contains(.,'Woonoppervlakte')]//parent::div/text()").get()
        if square:
            square_meters =  square.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters) 

        parking =response.xpath("//strong[contains(.,'Parkeerplek')]//parent::div/text()[.!=' -']").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking =response.xpath("//strong[contains(.,'Garages')]//parent::div/text()[.!=' -']").extract_first()    
            if parking:
                item_loader.add_value("parking", True)

        furnished = response.xpath("//strong[contains(.,'Gemeubileerd')]//parent::div/text()[contains(.,'Ja')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//strong[contains(.,'Lift')]//parent::div/text()[contains(.,'Ja')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
   
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[contains(@class,'lightbox_property_slider')]//@data-bg").extract()]
        if images:
                item_loader.add_value("images", images)

        available_date = response.xpath("//strong[contains(.,'Beschikbaarheid')]//parent::div/text()").extract_first() 
        if available_date:
            if "verhuurd" in available_date.lower():
                return
            else:
                if not "direct" in available_date.lower():
                    available_date = available_date.lower().replace("beschikbaar","").strip()
                    date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['nl'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
    
        item_loader.add_value("landlord_name", "Budget Housing")
        item_loader.add_value("landlord_phone", "+31 (0) 40 240 71 80")
        item_loader.add_value("landlord_email", "Info@budgethousing.nl")

        if title or desc or rent:
            yield item_loader.load_item()
        else:
            return