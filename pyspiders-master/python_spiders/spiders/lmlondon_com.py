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
    name = 'lmlondon_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lmlondon.com/property-to-rent/flat/any-bed/all-location?exclude=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lmlondon.com/property-to-rent/house/any-bed/all-location?exclude=1",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='card']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lmlondon_PySpider_united_kingdom")      

        item_loader.add_xpath("external_id", "substring-after(//div[contains(@class,'furtherdetails')]/ul/li[contains(.,'Reference')]//text(),': ')")

        title =" ".join(response.xpath("//h1//text()").extract())
        if title:
            item_loader.add_value("title",title)   
        address = response.xpath("//h1/text()").extract_first()
        if address:
            item_loader.add_value("address",address)   
                           
        rent = response.xpath("//div[@class='inner']/span[@class='price']/text()").extract_first()
        if rent:
            if "Weekly" in rent:
                rent = rent.split('£')[-1].strip().split(' ')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)

        room_count = response.xpath("//div[@class='container-div' and div/i[@class='fa fa-bed']]/p/text()").extract_first() 
        if room_count:   
            room_count = room_count.strip().split("bedroom")[0]
            item_loader.add_value("room_count", room_count)
        bathroom = response.xpath("//div[@class='container-div' and contains(.,'bathroom')]/p/text()").extract_first()
        if bathroom:            
            bathroom_count = bathroom.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count) 
  
        terrace = response.xpath("//div[@class='features']/ul/li[contains(.,'terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[@class='features']/ul/li[contains(.,'Balcony') or contains(.,'balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        swimming_pool = response.xpath("//div[@class='features']/ul/li[contains(.,'Pool ')]//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        furnished = response.xpath("//div[@class='features']/ul/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower() and "furnished or unfurnished" not in furnished:
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='features']/ul/li[contains(.,'Parking') or contains(.,'parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div[@class='desc']//text()[not(contains(.,'Arrange Viewing'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        available_date = " ".join(response.xpath("//div[contains(@class,'furtherdetails')]/ul/li[contains(.,'Availability:')]//text()").extract())
        if available_date:  
            date_parsed = dateparser.parse(available_date.split('Availability:')[1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                from datetime import date
                today = date.today()
                item_loader.add_value("available_date", today.strftime("%Y-%m-%d"))

        images = [response.urljoin(x) for x in response.xpath("//ul[@id='PropertyGallery']/li/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//div[@class='mapsEmbed']/iframe/@src").get()
        if script_map:
            latlng = script_map.split("maps?q=")[1].split("&z=")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
 
        item_loader.add_value("landlord_name", "Landmark Estates")
        item_loader.add_value("landlord_phone", "020 7515 0800")
       
        yield item_loader.load_item()