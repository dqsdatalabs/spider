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
import re

class MySpider(Spider):
    name = 'accordproperty_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom' 
    locale = 'en'
    external_source="Accordproperty_co_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.accordproperty.co.uk/rent/search/page-1/pricing-monthly/flat/exclude-letagreed/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.accordproperty.co.uk/rent/search/page-1/pricing-monthly/bungalow/house/exclude-letagreed/"
                ],
                "property_type": "house"
            }
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
        for item in response.xpath("//div[@class='card']/a/@href[not(contains(.,'property.detailsUrl'))]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("ref-")[1].split("/")[0])

        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        city=response.xpath("//title/text()").get()
        if city:
            city=city.split("|")[0].strip().split(" ")[-1]
            item_loader.add_value("city",city)


            
        detail = response.xpath("//script[@id='ng-details-panel']/text()").get()
        sel = Selector(text=detail, type='html')

        address = sel.xpath("//span[contains(@class,'panel__title-main')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            if address.count(",") >0:
                if ',' in address:
                    zipcode = address.split(",")[-1].strip()
                    zipcode_num = re.findall(r'\d+', zipcode)
                    if zipcode_num:
                        item_loader.add_value("zipcode", zipcode_num)
                    elif zipcode.isdigit():
                        item_loader.add_value("zipcode", zipcode)
                        
                else:
                    item_loader.add_value("zipcode", "B1 1LW")

        rent = sel.xpath("//p[contains(@class,'details-panel__details-text-primary')]/text()").get()
        if rent:
            rent = rent.split("pcm")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = sel.xpath("//div[contains(@class,'spec-list-icon')][contains(.,'Bedroom')]/following-sibling::span/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = sel.xpath("//div[contains(@class,'spec-list-icon')][contains(.,'Reception')]/following-sibling::span/text()[.!='0']").get()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = sel.xpath("//div[contains(@class,'spec-list-icon')][contains(.,'Bathroom')]/following-sibling::span/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        description = " ".join(response.xpath("//div[contains(@class,'copy__content')]//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        energy_label = response.xpath("//div[contains(@class,'copy__content')]//p//text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//div[contains(@class,'copy__content')]//p//text()[contains(.,' furnished') or contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//div[contains(@class,'copy__content')]//p//text()[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//div[contains(@class,'copy__content')]//p//text()[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]").get()
        if parking:
            if "no" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        available_date=response.xpath("//div[contains(@class,'copy__content')]//p//text()[contains(.,'Available from')]").get()
        if available_date:
            date2 =  available_date.split("from")[1]
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
        
        img_detail = response.xpath("//script[@id='ng-photo-slide']/text()").get()
        if img_detail:
            image_sel = Selector(text=img_detail, type='html')
            images = [response.urljoin(x) for x in image_sel.xpath("//li[@class='slide__item']//@data-image-path").getall()]
            if images:
                item_loader.add_value("images", images)
            
        latitude_longitude = response.xpath("//div/@data-location").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)    
        
        item_loader.add_value("landlord_name", "Accord Lets Lettings Birmingham")
        item_loader.add_xpath("landlord_phone", "//span[@class='details-frame__content-link-text details-frame__content-link-text--phone']//text()")
        item_loader.add_value("landlord_email","privacy@countrywide.co.uk")
        
        yield item_loader.load_item()