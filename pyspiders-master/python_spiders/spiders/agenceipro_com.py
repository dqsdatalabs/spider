# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'agenceipro_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agenceipro_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.agenceipro.com/locations/appartements/", "property_type": "apartment"},
	        {"url": "http://www.agenceipro.com/locations/maisons-villas/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for follow_url in response.xpath("//article[contains(@class,'property-item')]//h4/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agenceipro_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//h1[contains(@class,'page-title')]//span//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        latitude_longitude = response.xpath("//div[@id='property_map']/parent::div/script/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0].strip()
            longitude = latitude_longitude.split('lang":"')[1].split('"')[0].strip()  

        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_xpath("address", "//div[@class='page-breadcrumbs']//li[2]/a/text()")

        square_meters = response.xpath("//div[@class='property-meta clearfix']/span[1]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split('m2')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = "".join(response.xpath("//div[contains(@class,'property-meta')]/span[contains(.,'Bed')]//text()").extract())
        if room_count:
            room_count = room_count.strip().split('Bedroom')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//h5[@class='price']/span[2]/text()").get()
        if rent:
            rent = rent.strip().split('€')[0].replace(' ', '.').replace(',', '.')
            item_loader.add_value("rent", rent.split(".")[0])

        currency = 'EUR'
        item_loader.add_value("currency", currency)
        item_loader.add_xpath("utilities", "substring-before(substring-after(//div[@class='content clearfix']/p[contains(.,'Charges :')],': '),',')")

        external_id = response.xpath("//h5[@class='price']/parent::div/h4/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='content clearfix']//text()").getall()
        desc_html = ''          
        openwrite = False  
        if description:
            for d in description:
                if openwrite:
                    desc_html += d.strip()
                if d.find('Prestations') > -1:
                    openwrite = False
                if d.find('Description') > -1:
                    openwrite = True
            desc_html = desc_html.split('Prestations')[0]
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//nav[@class='property-breadcrumbs']/ul/li[2]/a/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@id='property-detail-flexslider']//a/@href[not(contains(.,'Capture-d'))]").getall()]
        if images:
            item_loader.add_value("images", images)


        floor_images = [x for x in response.xpath("//div[@id='property-detail-flexslider']//a/@href[contains(.,'Plan')]").getall()]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)

        item_loader.add_xpath("bathroom_count", "normalize-space(substring-before(//div[@class='property-meta clearfix']/span[contains(.,'Bathroom')]/text(),'Bathroom'))")

        parking = response.xpath("//div[@class='content clearfix']//text()").getall()
        parking1 = response.xpath("//ul/li/a[contains(.,'Parking')]/text()").get()
        if parking1:
            item_loader.add_value("parking", True)
        elif parking:
            for p in parking:
                if p.lower().find('parking') > -1 or p.lower().find('garage') > -1:
                    parking = True
                    item_loader.add_value("parking", parking)
                    break

        elevator = response.xpath("//div[@class='layoutArea']/div/p[contains(.,'Ascenseur')]").get()
        elevator1 = response.xpath("//ul/li/a[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        elif elevator1:
            item_loader.add_value("elevator", True)

        balcony = response.xpath("//div[@class='layoutArea']/div/p[contains(.,'Balcon')]").get()
        balcony1 = response.xpath("//ul/li/a[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elif balcony1:
            item_loader.add_value("balcony", True)
        
        swimming_pool = response.xpath("//div[@class='content clearfix']//text()").getall()
        if swimming_pool:
            for s in swimming_pool:
                if s.lower().find('pool') > -1:
                    swimming_pool = True
                    item_loader.add_value("swimming_pool", swimming_pool)
                    break
        
        deposit = response.xpath("//div/p//text()[contains(.,'de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            deposit = int(deposit) * int(rent.split(".")[0])
            item_loader.add_value("deposit", str(deposit))
        
        landlord_name = response.xpath("//h3[@class='title property-agent-title']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//ul[@class='contacts-list']/li/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(':')[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)
            
        item_loader.add_value("landlord_email", "contact@agenceiPro.com")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data