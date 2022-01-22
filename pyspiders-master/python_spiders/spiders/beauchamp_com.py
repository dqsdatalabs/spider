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
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'beauchamp_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.beauchamp.com/rent-property?p=1&property_type=5&rental_type=23",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.beauchamp.com/rent-property?p=1&rental_type=23&property_type%5B0%5D=3&property_type%5B1%5D=120&property_type%5B2%5D=9",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        any_url = response.meta.get("any_url", '')
        a = 1

        seen = False
        for item in response.xpath("//ul[contains(@class,'products-list')]/li//a[@title='View Full Details']/@href").extract():
            f_url = response.urljoin(item)
            if a == 1:
                if f_url == any_url:
                    return
                any_url = f_url
            a += 1
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
            
        
        if page == 2 or seen:
            p_url = ''
            if 'property_type=5' in response.url:
                p_url = f'https://www.beauchamp.com/rent-property?p={page}&property_type=5&rental_type=23'
            else:
                p_url = f'https://www.beauchamp.com/rent-property?p={page}&rental_type=23&property_type[0]=3&property_type[1]=120&property_type[2]=9'
            yield Request(
                p_url, 
                callback=self.parse, 
                meta={"property_type" : response.meta.get("property_type"), "page":page+1, "any_url":any_url},
            )  
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Beauchamp_PySpider_"+ self.country + "_" + self.locale)
        address = response.xpath("normalize-space(//div[@id='property-address']/text())").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[0].strip())



        zipcode = response.xpath("//div[@class='property-reference']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("Property reference:"))

        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"lat":')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('"lng":')[1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            # geolocator = Nominatim(user_agent=response.url)
            # location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
            # if location.address:
            #     address = location.address
            #     if 'postcode' in location.raw['address']:
            #         if location.raw['address']['postcode']:
            #             zipcode = location.raw['address']['postcode']
            #     else:
            #         zipcode = None
            #     if 'city' in location.raw['address']:
            #         if location.raw['address']['city']:
            #             city = location.raw['address']['city']
            #     else:
            #         city = None
            #     if 'municipality' in location.raw['address']:
            #         if location.raw['address']['municipality']:
            #             city = location.raw['address']['municipality']
            #     else:
            #         city = None
            # else:
            #     address = None
            # if address:
            #     item_loader.add_value("address", address)
            # if zipcode:
            #     item_loader.add_value("zipcode", zipcode)
            # if city:
            #     item_loader.add_value("city", city)
        
        bathroom_count = response.xpath("//span[contains(@class,'bathrooms')]/text()").get()
        if bathroom_count:
            if bathroom_count.strip() == "Bathroom":
                item_loader.add_value("bathroom_count", "1")
            else:
                item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())

        square_meters = response.xpath("//span[contains(.,'Living Area')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split('sq')[0].strip().replace('\xa0', '').replace(',', '')
            square_meters = str(int(float(square_meters) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
            if room_count.isnumeric():
                room_count = str(int(float(room_count)))
                item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[@class='base-price']/text()").get()
        
        if rent != "P.O.A":
            if 'week' in rent.lower():
                rent = rent.split('£')[-1].split('/')[0].strip().replace('\xa0', '').replace(',', '')
                if rent.isnumeric():
                    rent = str(int(rent) * 4)
            else:
                rent = rent.split('£')[-1].split('/')[0].strip().replace('\xa0', '').replace(',', '')
                if rent.isnumeric():
                    rent = str(int(rent))
            if rent and rent != '0':
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')

        external_id = response.xpath("//div[@class='property-reference']/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@id='property-description']//text()").getall()
        if description:
            desc = " ".join(description)
            item_loader.add_value("description", desc)
   

        furnished = response.xpath("//div[@id='property-description']/ul/li[contains(.,'Furnished')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        # images = [response.urljoin(x)for x in response.xpath("//div[@id='detail-slider']/ul[@class='slides']/li//source[@class='tablet']/@data-srcset").extract()]
        # if images:
        #     item_loader.add_value("images", images)
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Elevator')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        furnished = "".join(response.xpath("//div[@id='property-description']/p[contains(.,'furnished')]/text()").getall())
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_name = response.xpath("//div[@id='agent-name']/text()").get()
        if landlord_name:         
            if len(landlord_name)==1:
                item_loader.add_value("landlord_name","Beauchamp Estates")
            else:
                landlord_name = landlord_name.strip()
                item_loader.add_value("landlord_name", landlord_name)

        
        landlord_phone = response.xpath("//div[@id='agent-telephone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)



        landlord_email = response.xpath("//div[@id='agent-email']/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", landlord_email)

        img = response.xpath("//div[@class='product-info']//div[@class='price-box']/span/@id").extract_first()
        img_id = img.split("-")[-1].strip()
        
        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        yield FormRequest(
            f"https://www.beauchamp.com/imageloader/image/carousel?product_id={img_id}",
            callback=self.parse_image,
            headers=headers,
            dont_filter=True,
            meta={
                "item_loader" : item_loader
            })

    def parse_image(self,response):
        item_loader = response.meta.get("item_loader")
        img =[]
        j_seb = json.loads(response.body)
        for j in j_seb["thumbnail"]:
            image = j["src"]
            img.append(image)
        item_loader.add_value("images", img)  


        yield item_loader.load_item()