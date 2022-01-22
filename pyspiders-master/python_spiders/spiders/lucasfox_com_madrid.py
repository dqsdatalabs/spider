# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'lucasfox_com_madrid'
    execution_type='testing'
    country='spain'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=74B1920785&locationid=821FAC05F2",
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=57D53735E2&locationid=821FAC05F2",
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=88D6CB38FF&locationid=821FAC05F2"
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=0053BE9BFF&locationid=821FAC05F2",
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=A074A6CF72&locationid=821FAC05F2",
                    "https://www.lucasfox.com/search.html?classid=res_rent&typeid=6B34D377FF&locationid=821FAC05F2",

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

        for item in response.xpath("//li[@class='c-listing__item']"):
            follow_url = response.urljoin(item.xpath(".//p[@class='c-listing__title']/a/@href").get())
            address = item.xpath(".//p[@class='c-listing__location']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "address" : address})

        
        next_page = response.xpath("//a[.='Next »']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Lucasfoxmadrid_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_css("title","title")
        item_loader.add_value("address", response.meta.get("address"))
        
        latitude_longitude = response.xpath("//script[contains(.,'hasMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('/maps/@')[1].split(',')[0]
            longitude = latitude_longitude.split('/maps/@')[1].split(',')[1].split(',')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        zipcode=response.xpath("//ul[@class='c-crumb']/li/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
        square_meters = response.xpath("//span[.='Size']/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[.='Bedrooms']/following-sibling::span/text()").extract_first()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//div[@class='c-detail-showcase-titles__price']/text()").extract_first()
        if rent:
            rent = rent.strip().split(' ')[1].replace(',', '')
            item_loader.add_value("rent", rent)

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//span[@class='c-detail-showcase-titles__ref']/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='c-detail-body__intro']//p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        if "washer" in desc_html:
            item_loader.add_value("washing_machine", True)
        if "lift" in desc_html:
            item_loader.add_value("elevator", True)
        if "furnished" in desc_html:
            item_loader.add_value("furnished", True)
            
        city = response.xpath("//ul[@class='c-crumb']/li[2]/a/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//img[@class='c-listing-card__image']/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//img[@class='energy-cert']/@alt").get()
        if energy_label:
            energy_label = energy_label.strip().split(' ')[-1]
            item_loader.add_value("energy_label", energy_label)

        parking = response.xpath("//li[@class='c-detail-features-list__item' and contains(.,'Parking')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//li[@class='c-detail-features-list__item' and contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[@class='c-detail-features-list__item' and contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[@class='c-detail-features-list__item' and contains(.,'Swimming pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)
        
        floor_plan_images=response.xpath("//ul[@class='c-link-list']/li/a/@title[contains(.,'Floor')]/parent::a/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        bathroom=response.xpath("//ul[contains(@class,'detail')]/li/span[contains(.,'Bathroom')]/following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        pets_allowed=response.xpath("//ul[contains(@class,'detail')]/li[contains(.,'Pet-friendly')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        item_loader.add_value("landlord_name", "LUCAS FOX INTERNATIONAL PROPERTIES")
        
        landlord_phone = response.xpath("//ul[@class='c-link-list']/li/a/@href[contains(.,'tel')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.replace("tel:+",""))

        landlord_email = response.xpath("//ul[@class='c-link-list']/li/a/@href[contains(.,'mail')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.replace("mailto:",""))
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
        
       

        
        
          

        

      
     