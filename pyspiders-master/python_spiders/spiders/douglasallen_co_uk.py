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
    name = 'douglasallen_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ',' 
    scale_separator = '.'
    external_source="Douglasallen_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.douglasallen.co.uk/properties/list/?SaleTypeID_a=2&SaleTypeID=2&Location=&no-name-min=&MinPrice=&no-name=&MaxPrice=&HouseTypeID=2&Bedrooms=&DistanceInMiles=2&AddedAge=&Submitted=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.douglasallen.co.uk/properties/list/?SaleTypeID_a=2&SaleTypeID=2&Location=&no-name-min=&MinPrice=&no-name=&MaxPrice=&HouseTypeID=1&Bedrooms=&DistanceInMiles=2&AddedAge=&Submitted=1",
                    "https://www.douglasallen.co.uk/properties/list/?SaleTypeID_a=2&SaleTypeID=2&Location=&no-name-min=&MinPrice=&no-name=&MaxPrice=&HouseTypeID=3&Bedrooms=&DistanceInMiles=2&AddedAge=&Submitted=1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='card__image-main']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            prop_type = response.url.split("HouseTypeID=")[1].split("&")[0]
            url = f"https://www.douglasallen.co.uk/properties/list/?SaleTypeID_a=2&SaleTypeID=2&Location=&no-name-min=&MinPrice=&no-name=&MaxPrice=&HouseTypeID={prop_type}&Bedrooms=&DistanceInMiles=2&AddedAge=&Submitted=1&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        room_count = response.xpath("//li[span[.='Bedrooms']]/span[@class='stat-value']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.replace("x",""))    
        title=response.xpath("//h1/text()").get()
        if title and not "sorry" in title.lower():
            item_loader.add_value("title", title)    
        neglect=" ".join(response.xpath("//h4/text()").getall())
        if "we dont have any properties" in neglect or "we do not have any properties" in neglect:
            return  
        
        item_loader.add_xpath("rent_string","//p[@class='property-main-price']/span[@class='amount']/text()")
        address = response.xpath("//h2[@class='property-sub-title']//text()").get()
        if address:
            item_loader.add_value("address", address.strip())    
            item_loader.add_value("city", address.split(",")[-1].strip())    

        description = " ".join(response.xpath("//div[@id='property-description']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='js-details-property-thumb-slider']/div//img[@alt!='Floor Plan']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='js-details-property-thumb-slider']/div//img[@alt='Floor Plan']/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
     
        item_loader.add_xpath("landlord_phone", "//div[@class='row detail-info-boxes']//div[@class='number-box']/a//p/text()[normalize-space()]")     
        item_loader.add_xpath("landlord_name", "//div[@class='row detail-info-boxes']//p/strong/text()")
        furnished = response.xpath("//li/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)  
        parking = response.xpath("//li/text()[contains(.,'Parking') or contains(.,'garage')]").get()
        if parking:
            item_loader.add_value("parking", True) 
        location=response.xpath("//img[@id='smallMap']/@link").get()
        if location:
            longitude=location.split("center=")[-1].split("&markers")[0]
            if longitude:
                item_loader.add_value("longitude",longitude.split(",")[0])
            latitude=longitude.split(",")[-1]
            if latitude:
                item_loader.add_value("latitude",latitude)
 
        item_loader.add_xpath("latitude", "//div[@id='map']/@data-center-lat")
        item_loader.add_xpath("longitude", "//div[@id='map']/@data-center-lng")
        yield item_loader.load_item()