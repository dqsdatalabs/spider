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
from word2number import w2n
import dateparser

class MySpider(Spider):
    name = 'godfreyandbarr_com'   
    execution_type='testing'
    country='united_kingdom'
    locale='en'
     
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.godfreyandbarr.com/search/?showstc=on&showsold=on&sold_at_weeks=4&instruction_type=Letting&property_type%21=Land%2CInvestment&minpricew=&maxpricew=&keyword=&property_type=Flat%2CApartment%2CMaisonette&bedrooms=&receptions=&bathrooms=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.godfreyandbarr.com/search/?showstc=on&showsold=on&sold_at_weeks=4&instruction_type=Letting&property_type%21=Land%2CInvestment&minpricew=&maxpricew=&keyword=&property_type=Semi-Detached%2CSemi-Detached+Bungalow%2CDetached%2CBungalow%2CTerraced%2CTown+House&bedrooms=&receptions=&bathrooms=",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='thumbResult']"):
            follow_url = response.urljoin(item.xpath("./div[@class='thumbsImage']/a/@href").get())
            address = item.xpath("./div[@class='thumbsAddress']/h3/text()[1]").get()
            rent = item.xpath("./div[@class='thumbsAddress']/h3/text()[2]").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'address': address, 'rent': rent})
        
        next_page = response.xpath("//a[.='>>']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        let = response.xpath("//div[@class='resultcornerFlash']/a/img/@src[contains(.,'let')]").extract_first()
        if let:
            pass
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)

            item_loader.add_value("external_source", "Godfreyandbarr_PySpider_"+ self.country + "_" + self.locale)

            external_id = response.url.split('property-details/')[-1].split('/')[0].strip()
            if external_id:
                item_loader.add_value("external_id", external_id)
            
            address = response.meta.get('address')
            if address:
                item_loader.add_value("address", address.strip())
                item_loader.add_value("zipcode", address.strip().split(' ')[-1].strip())
            
            city = response.xpath("//div[@id='fullDetails']/h1/span[1]/text()").extract_first()
            if city:
                item_loader.add_value("city", city.split(" ")[-1])
                
            title = " ".join(response.xpath("//h1//text()").getall()).strip()
            if title:
                item_loader.add_value("title", title.replace('\xa0', ''))

            description = " ".join(response.xpath("//div[@id='propDescrip']//text()").getall()).strip()
            if description:
                item_loader.add_value("description", description.replace('\xa0', ''))

                if 'sq ft' in description:
                    square_meters = description.split('sq ft')[0].strip().split(' ')[-1]
                    if square_meters.isnumeric():
                        item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))

            room_count = response.xpath("//span[contains(.,'Bedroom')]/text()").get()
            if room_count:
                try:
                    room_count = w2n.word_to_num(room_count.lower().split('bedroom')[0].strip())
                    item_loader.add_value("room_count", str(room_count))
                except:
                    pass
            else:
                if description:
                    if 'bedroom' in description.lower():
                        try:
                            room_count = w2n.word_to_num(description.split('bedroom')[0].strip().split(' ')[-1])
                            item_loader.add_value("room_count", str(room_count))
                        except:
                            pass
            
            bathroom_count = response.xpath("//span[contains(.,'Bathroom')]/text()").get()
            if bathroom_count:
                try:
                    bathroom_count = w2n.word_to_num(bathroom_count.lower().split('bathroom')[0].strip())
                    item_loader.add_value("bathroom_count", str(bathroom_count))
                except:
                    pass
            else:
                if description:
                    if 'bathroom' in description.lower():
                        try:
                            bathroom_count = w2n.word_to_num(description.split('bathroom')[0].strip().split(' ')[-1])
                            item_loader.add_value("bathroom_count", str(bathroom_count))
                        except:
                            pass
            
            rent = response.meta.get('rent')
            if rent:
                rent = rent.split('£')[-1].lower().split('per')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            
            available_date = response.xpath("//span[contains(.,'Available')]/text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.lower().split('available')[-1].strip(), date_formats=["%d %B %Y"], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
    
            images = [response.urljoin(x) for x in response.xpath("//div[@id='propMainImg']//img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='propThumbImgs']//a[contains(@href,'floorplan')]/img/@src").getall()]
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            
            latitude_longitude = response.xpath("//script[contains(.,'ShowMap')]/text()").get()
            if latitude_longitude:
                item_loader.add_value("latitude", latitude_longitude.split('&q=')[1].split('%2C')[0].strip())
                item_loader.add_value("longitude", latitude_longitude.split('&q=')[1].split('%2C')[1].split('"')[0].strip())


            unfurnished = response.xpath("//div[@id='propDescrip']/h3[contains(.,'UNFURNISHED')]/text()").get()
            if unfurnished:
                item_loader.add_value("furnished", False)
            else:

                furnished = response.xpath("//span[contains(.,'Furnished')]").get()
                if furnished:
                    item_loader.add_value("furnished", True)
                else:
                    furnished = response.xpath("//div[@id='propDescrip']/h3[contains(.,'FURNISHED')]/text()").get()
                    if furnished:
                        item_loader.add_value("furnished", True)

            
            parking = response.xpath("//span[contains(.,'No Parking')]").get()
            if parking:
                item_loader.add_value("parking", False)
            else:
                parking = response.xpath("//span[contains(.,'Parking') or contains(.,'parking')]/text()").get()
                if parking:
                    item_loader.add_value("parking", True)
            
            balcony = response.xpath("//span[contains(.,'Balcon')]").get()
            if balcony:
                item_loader.add_value("balcony", True)
            
            elevator = response.xpath("//span[contains(.,'Lift')]").get()
            if elevator:
                item_loader.add_value("elevator", True)

            terrace = response.xpath("//span[contains(.,'terrace') or contains(.,'Terrace')]").get()
            if terrace:
                item_loader.add_value("terrace", True)
            
            swimming_pool = response.xpath("//span[contains(.,'Swimming Pool')]").get()
            if swimming_pool:
                item_loader.add_value("swimming_pool", True)

            item_loader.add_value("landlord_phone", "+44 (0) 20 8959 9000")
            item_loader.add_value("landlord_email", "enquire@godfreyandbarr.com")
            item_loader.add_value("landlord_name", "Godfrey and Barr Estate Agents")

            yield item_loader.load_item()
