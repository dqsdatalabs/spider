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
from  geopy.geocoders import Nominatim
import dateparser

class MySpider(Spider):
    name = 'northwooduk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.northwooduk.com/properties/lettings/tag-flat", "property_type": "apartment"},
	        {"url": "https://www.northwooduk.com/properties/lettings/tag-house", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='col-xl-4 col-md-6 col']/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            prop = response.meta.get('property_type').replace("apartment","flat")
            url = f"https://www.northwooduk.com/properties/lettings/tag-{prop}/?pg={page}&drawMap="
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Northwooduk_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title//text()")
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("-")[-2]+"-"+external_id.split("-")[-1].replace("/",""))
        dontallow="".join(response.xpath("//div[@class='price']/span[@class='qualifier']/text()").getall())
        if dontallow and "commercial" in dontallow:
            return 
        if response.xpath("//span[@class='qualifier']/text()[contains(.,'PCM')]").get():
            rent="".join(response.xpath("//div[@class='price']/span[@class='amount']/text()").getall())
            currency=response.xpath("//h2/meta[@itemprop='priceCurrency']/@content").get()
            if rent:
                rent = rent.strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
                item_loader.add_value("rent_string", rent)
        if response.xpath("//span[@class='qualifier']/text()[contains(.,'Per Week')]").get():
            rent="".join(response.xpath("//div[@class='price']/span[@class='amount']/text()").getall())
            currency=response.xpath("//h2/meta[@itemprop='priceCurrency']/@content").get()
            if rent:
                rent = rent.strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '').replace("£","")
                item_loader.add_value("rent", int(rent)*4)
        
   
        room_count = response.xpath("//span[@title='Public Rooms']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            item_loader.add_xpath("room_count","//span[@title='Bedrooms']/span/text()")

        bathroom_count = response.xpath("//span[@title='Bathrooms']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        latitude_longitude=response.xpath("//script[contains(.,'geo')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude')[1].split(',')[0].strip().replace('"',"").replace(":","").strip()
            longitude = latitude_longitude.split('longitude')[1].split('}')[0].replace('"',"").replace(":","").strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        address="".join(response.xpath("//h1[@class='property-address']//text()").getall()).strip()
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address))

        deposit="".join(response.xpath("//div[@class='accordion-content']/ul/li[span[.='Deposit:']]/text()").getall()).strip()
        if deposit:
            dep = deposit.replace(",","").strip()
            item_loader.add_value("deposit",dep)


        city_zipcode = "".join(response.xpath("//h1[@class='property-address']/div[@class='addr2']/text()").getall()).strip()
        if city_zipcode:     
            zipcode = city_zipcode.split(",")[-1].strip()
            city=city_zipcode.split(",")[0].strip()
            item_loader.add_value("zipcode", zipcode) 
            item_loader.add_value("city", city)
        available_date=response.xpath("//div[@class='availability']/text()").get()
        if available_date:
            available_date=available_date.split(":")[-1].replace(",","")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        
        desc="".join(response.xpath("//div[@class='accordion-content']/div//text()").getall()).strip()
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        if "Private garage" in desc:
            return 
        parking=" ".join(response.xpath("//h3[.='Features']/following-sibling::ul//li//text()").getall())
        if parking and "parking" in parking.lower():
            item_loader.add_value("parking",True)

            
        if "sq.ft" in desc:
            square_meters=desc.split("sq.ft")[0].split("(")[1].strip()
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        elif "m2" in desc:
            square_meters=desc.split("m2")[0].split(":")[-1].strip()
            item_loader.add_value("square_meters", square_meters)

        
        images=[x for x in response.xpath("//div[@class='slide-main-wrapper']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//div[@class='accordion-content']/ul/li[.='Furnished']/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
            
            
        item_loader.add_value("landlord_name","Nortwood")
        item_loader.add_xpath("landlord_email","//a[@class='contact-text-link']/text()")
        item_loader.add_xpath("landlord_phone", "//a[@class='contact-text-link InfinityNumber']/text()")
        
        yield item_loader.load_item()