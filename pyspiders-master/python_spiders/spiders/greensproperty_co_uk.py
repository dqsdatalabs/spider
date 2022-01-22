# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'greensproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Greensproperty_Co_PySpider_united_kingdom'
    start_urls = ["https://www.greensproperty.co.uk/residential-lettings?parent_category=&view=grid&location=&latitude=&longitude=&distance=6&propertyTypes%5B%5D=&minPrice=&maxPrice=&minBedrooms=&maxBedrooms="]

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='col-24 col-md-12 col-lg-8 card-wrapper']//div[@class='card']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            
            seen = True
            
        if page == 2 or seen:
            f_url = f"https://www.greensproperty.co.uk/residential-lettings?page={page}&view=grid&distance=6"
            yield Request(
                f_url,
                callback=self.parse
            ) 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='col-24 text-center  ']/h1/text()").get()
        if status and "page not found" in status.lower():
            return
        item_loader.add_value("external_link", response.url)
        property_type=response.xpath("//h1/text()").get()
        if property_type and "house" in property_type.lower():

           item_loader.add_value("property_type", "House") 
        if property_type and "flat" in property_type.lower():

           item_loader.add_value("property_type", "Apartment")
        if property_type and "studio" in property_type.lower():

           item_loader.add_value("property_type", "studio")
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("title", "//h1/text()")
        external_id = response.xpath("//meta[@property='og:url']//@content").get()
        if external_id and "share" in external_id.lower():
            item_loader.add_value("external_id", external_id.split('share/')[-1].split('-')[0])
        elif external_id and "flat" in external_id.lower():
            item_loader.add_value("external_id", external_id.split('flat/')[-1].split('-')[0])
        elif external_id and "studio" in external_id.lower():
            item_loader.add_value("external_id", external_id.split('studio/')[-1].split('-')[0])
        
        address = response.xpath("//div[contains(@class,'order-xl-5 text-center')]/h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip().split(' ')[0])
        zip = response.xpath("//div[@class='col-24 order-xl-5 text-center']/h2/text()").get()
        if zip:
            zip = zip.split(',')[-1].strip().split()
            if zip:
                l = len(zip)
                zipcode = zip[l-2] + " " + zip[l-1]
                item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//p[@class='property-price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("£","").replace(",","").strip())
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//h1/text()[contains(.,'Bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        # bathroom_count = response.xpath("//div[@class='property-header-key']//li[contains(@class,'item_bath')]/strong/text()").get()
        # if bathroom_count:
        #     item_loader.add_value("bathroom_count", bathroom_count)
        
        description = " ".join(response.xpath("//div[@id='tab-details']//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        images = [x for x in response.xpath("//a[contains(@class,'property-carousel')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//p/text()[contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split("£")[1].strip()
            item_loader.add_value("deposit", deposit)
        
        parking = response.xpath("//li/text()[contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,' furnished') or contains(.,'Furnished')][not(contains(.,'unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True) 
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        energy_label = response.xpath("//div[contains(@class,'eerc')]/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        latlng = response.xpath("//a[@class='js-tab-link js-load-gmaps']/img/@data-lazy-img").get()
        if latlng:
            latitude = latlng.split('center=')[-1].split(',')[0]
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split(',')[-1].split('&')[0]
            item_loader.add_value("longitude", longitude)

        import dateparser
        available_date = response.xpath("//span[@class='property-available']/text()").get()
        if available_date:
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "Greens Property")
        item_loader.add_value("landlord_phone", "0208 819 14 99")
        item_loader.add_value("landlord_email", "lettings@greensproperty.co.uk")

        yield item_loader.load_item()