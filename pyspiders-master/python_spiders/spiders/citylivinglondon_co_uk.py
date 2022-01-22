# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import json
from scrapy import Request,FormRequest 


class CitylivinglondonSpider(scrapy.Spider):
    name = "citylivinglondon_co_uk"
    allowed_domains = ["citylivinglondon.co.uk"]
    # start_urls = ['http://www.citylivinglondon.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source="Citylivinglondon_PySpider_united_kingdom_en"

    def start_requests(self):
        form_data={
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "72a2ec8d-1370-4ccc-8daa-54bf113c06a3",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
            "includeSoldButton": "false",
            "page":"1",
        }
        url = "https://www.citylivinglondon.co.uk/api/set/results/grid"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=form_data,
        )

    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='property-description-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
         
        if page == 2 or seen:
            form_data={
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "72a2ec8d-1370-4ccc-8daa-54bf113c06a3",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
                "includeSoldButton": "false",
                "page":str(page),
            }  

            yield FormRequest(
                "https://www.citylivinglondon.co.uk/api/set/results/grid", 
                callback=self.parse,
                formdata=form_data,
               
            )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url  
        if external_link:
            item_loader.add_value("external_link",external_link)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("property/")[-1].split("/london")[0].split("/")[0])
        city = response.xpath('//span[@class="city"]/text()').get().split(",")[0]
   
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value('title', title)
        item_loader.add_xpath('images', './/div[@class="img-gallery"]//div[@class="item"]/div/@data-bg')
        item_loader.add_xpath('floor_plan_images', './/h2[contains(text(),"Floorplans")]/..//img[@title="floorplan"]/@data-src')

        room_count = max(int(response.xpath('.//li[@class="FeaturedProperty__list-stats-item"]/span/text()').extract()[0]),1)
        item_loader.add_value('room_count', str(room_count))

        bathroom_count = response.xpath('.//li[@class="FeaturedProperty__list-stats-item"]/span/text()').extract()[1]
        item_loader.add_value('bathroom_count', bathroom_count)

        currency = response.xpath('//span[@class="nativecurrencysymbol"]/text()').extract_first()
        rent = response.xpath('//span[@class="nativecurrencyvalue"]/text()').extract_first()
        rent = str(int(rent)*4)
        rent_string = currency+rent
        item_loader.add_value('rent_string', rent_string) 

        item_loader.add_xpath('description', './/section[@id="description"]//p/text()')
        item_loader.add_value('city', city)
        address=response.xpath("//span[@class='address1']/text()").get()
        if address:
            item_loader.add_value('address', address)
        zipcode=response.xpath("//span[@class='displayPostCode']/text()").get()
        if zipcode:
            item_loader.add_value('zipcode', zipcode)
        latitude=response.xpath("//section[@id='maps']/@data-cords").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("lat")[-1].split(",")[0].split(":")[-1].replace('"',"").replace("{","").replace("}",""))
        longitude=response.xpath("//section[@id='maps']/@data-cords").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("lng")[-1].split(":")[-1].replace('"',"").replace("{","").replace("}",""))

        item_loader.add_value('landlord_phone', '0207 351 6100')
        item_loader.add_value('landlord_email', 'info@citylivinglondon.co.uk')
        item_loader.add_value('landlord_name', 'City Living London')

        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage', 'student property']
        studio_types = ["studio"]
        
        # property_type
        property_type = " ".join(response.xpath("//section[@id='description']//p/text()").getall())
        if property_type:
            if any(i in property_type.lower() for i in studio_types):
                item_loader.add_value('property_type', 'studio')
            elif any(i in property_type.lower() for i in apartment_types):
                item_loader.add_value('property_type', 'apartment')
            elif any(i in property_type.lower() for i in house_types):
                item_loader.add_value('property_type', 'house')
            else:
                return

        features = ' '.join(response.xpath('.//a[contains(text(),"Main Features")]/../../div//li/text()').extract())
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        
        # "http://www.citylivinglondon.co.uk/property/30100749/w2/london/devonshire-terrace/studio/studio"
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)
        
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)
        
        # "http://www.citylivinglondon.co.uk/property/29899859/sw5/london/courtfield-gardens/studio/studio"
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        
        if "furnished" in features.lower():
            item_loader.add_value('furnished', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()
