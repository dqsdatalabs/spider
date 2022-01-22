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
    name = 'bruntonresidential_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://bruntonresidential.com/property-to-rent/apartment/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/end-of-terrace-house/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/flat/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/maisonette/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/penthouse/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/terraced-bungalow/any-bed/all-location?exclude=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://bruntonresidential.com/property-to-rent/detached-bungalow/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/detached-house/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/semi-detached-bungalow/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/semi-detached-house/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/terraced-house/any-bed/all-location?exclude=1",
                    "https://bruntonresidential.com/property-to-rent/town-house/any-bed/all-location?exclude=1"

                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://bruntonresidential.com/property-to-rent/studio-flat/any-bed/all-location?exclude=1",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='card']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        lat_agreed = response.xpath("//main/div[@class='displayStatus']/span[.='Let Agreed']/text()").extract_first()
        if lat_agreed:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Bruntonresidential_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")


        external_id = "".join(response.xpath("//div[@class='furtherdetails span4']/ul/li[span[contains(.,'Reference')]]").getall())
        if external_id:
            ref = external_id.split(":")[1].strip()
            item_loader.add_value("external_id",ref) 

        rent = response.xpath("//span[@class='price']/span/text()").get()
        if rent:  
            price = rent.replace(",","")                  
            item_loader.add_value("rent_string",price)

        address = " ".join(response.xpath("substring-after(//h1[@class='displayname']/text(),'in ')").getall())
        if address:  
            zipcode= address.split(",")[-1].strip()            
            city= address.split(",")[-2].strip()            
            item_loader.add_value("address",address)
            item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("city",city)

        room_count = "".join(response.xpath("//div[@class='span4']/div[div[i[@class='fa fa-bed']]]/p/text()").getall())
        if room_count:
            room = room_count.split("bedroom")[0].strip()
            item_loader.add_value("room_count",room) 

        available_date=response.xpath("//div[@class='furtherdetails span4']/ul/li[span[contains(.,'Availability')]]/text()").get()
        if available_date:
            date2 = available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        bathroom_count = "".join(response.xpath("//div[@class='span4']/div[div[i[@class='fa fa-bathtub bathroom']]]/p/text()").getall())
        if bathroom_count:
            bath = bathroom_count.split("bathroom")[0].strip()
            item_loader.add_value("bathroom_count",bath) 

        item_loader.add_xpath("latitude","substring-before(substring-after(//iframe/@src[contains(.,'map')],'?q='),',')") 
        item_loader.add_xpath("longitude","substring-before(substring-after(//iframe/@src[contains(.,'map')],','),'&z')") 

        description = " ".join(response.xpath("//div[@class='desc']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//section[@id='property-images']/div/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = "".join(response.xpath("//div[@class='span4']/div[div[@class='image couch']]/p/text()").getall())
        if furnished:
            if "Furnished" in furnished:
                item_loader.add_value("furnished",True)
            elif "Unfurnished" in furnished:
                item_loader.add_value("furnished",False)


        item_loader.add_value("landlord_phone", "0191 236 8347")
        item_loader.add_value("landlord_name", "Brunton Residential")
        item_loader.add_value("landlord_email", "info@bruntonresidential.com")

        yield item_loader.load_item()