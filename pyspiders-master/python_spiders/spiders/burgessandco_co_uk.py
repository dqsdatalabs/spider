# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'burgessandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
  
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.burgessandco.co.uk/property_area/to-rent/",
                    "https://www.burgessandco.co.uk/property_area/to-rent/?wppf_property_type=flat&wppf_radius=10&wppf_soldlet=hide&wppf_search=to-rent&wppf_orderby=price-desc&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_records=12",
                ],

            },	     
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='wppf_detail']//p/a[@class='wppf_more']/@href").getall():       
            yield Request(url, callback=self.populate_item,)
  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = " ".join(response.xpath("//h1//text()").getall())
        if status and "let agreed" in status.lower():
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Burgessandco_Co_PySpider_united_kingdom") 

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        property_type=" ".join(response.xpath("//h1//text()").getall())
        if property_type:
            if "flat" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "apartment" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "house" in property_type.lower():
                item_loader.add_value("property_type","house")
            
            

        address = " ".join(response.xpath("//div[contains(@class,'wppf_property_title')]//h2/text()").getall())
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)

        zipcode = response.xpath("//meta[@property='og:description'][2]/@content").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip()
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(@class,'wppf_property_title')]//h2//span//text()").get()
        if rent:
            rent = rent.strip().split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property_about')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//strong[contains(.,'Bedroom')]//following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//strong[contains(.,'Bathroom')]//following-sibling::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'slideshow')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        item_loader.add_value("external_id", external_id)
        
        item_loader.add_value("landlord_name", "BURGESS & CO")
        item_loader.add_value("landlord_phone", "01424 533 555")
        item_loader.add_value("landlord_email", "lettings@burgessandco.co.uk")
   
        yield item_loader.load_item()