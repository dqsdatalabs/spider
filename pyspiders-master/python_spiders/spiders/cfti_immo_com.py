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
from html.parser import HTMLParser
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'cfti_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cftiimmo_PySpider_france_fr'
    page_control = 1

    def start_requests(self):
        yield Request(
                "https://www.cfti-immo.com/fr/location/1/",
                callback=self.get_token,
            )
    
    def get_token(self, response):
        token = response.xpath("//input[@id='property_search__token']/@value").get()
        start_urls = [
            {
                "type" : "maison",
                "property_type" : "house"
            },
            {
                "type" : "appartement",
                "property_type" : "apartment"
            },
        ]

        for url in start_urls:
            r_type = url.get("type")
            payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[_token]\"\r\n\r\n{token}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[typeTransac]\"\r\n\r\nlocation\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[type][]\"\r\n\r\n{r_type}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[budgetMin]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[budgetMax]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[nbRoom]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[nbBedRoom]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[surfaceMin]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[ref]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
            yield Request(url="https://www.cfti-immo.com/fr/location/1/",
                                 callback=self.parse,
                                 body=payload,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//ul[@class='properties']/li[@data-lat]"):
            f_url = response.urljoin(item.xpath(".//a[contains(.,'détails')]/@href").get())
            lat = item.xpath("./@data-lat").get()
            lng = item.xpath("./@data-lng").get()
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "lat" : lat,
                    "lng" : lng,
                },
            )
        
        next_page = response.xpath("//a[.='>']/@href").get()
        next_page_number = int(next_page.split("/")[-2])
        if self.page_control < next_page_number:
            self.page_control = next_page_number
            headers = {
                'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
                'cache-control': "no-cache",
                'Referer': response.url,
                'Host': 'www.cfti-immo.com'
            }
            yield Request(
                url=response.urljoin(next_page),
                headers=headers,
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cftiimmo_PySpider_"+ self.country + "_" + self.locale)

        

        square_meters = response.xpath("//p[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//p[contains(.,'Pièce')]/span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//p[@class='h4']/span/parent::p/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//p[@class='h4']/span/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@class='read-more']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if "\u00e9tage" in desc_html:
            floor = desc_html.split("\u00e9tage")[0].replace("et dernier", "").strip().split(" ")[-1]
            floor = floor.replace("ème","").replace("er","").replace("e","").replace("m","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        images = [urljoin('https://www.cfti-immo.com', x) for x in response.xpath("//div[@id='choco-gallery']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))

        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'large-4')]/p[contains(.,'Salle(s) de bains')]/span/text()")
        
        deposit = response.xpath("//p[contains(.,'Dépôt de garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//p[contains(.,'Honoraires locataire TTC')]/span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0]
            utilities = int(float(utilities))
            item_loader.add_value("utilities", utilities)

        address = response.xpath("//h2[@class='h4']/span/text()").extract_first()
        if address:
            address = " ".join(address.split(" ")[1:])
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split("(")[1].replace(")",""))
            item_loader.add_value("city", address.split("(")[0])

        studio = response.xpath("//div[@class='title']/h1/text()").extract_first()
        if "Studio" in studio:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))


        parking = response.xpath("//p[contains(.,'Parking')]/span/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//p[contains(.,'Ascenseur')]/span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        landlord_name = response.xpath("//li/p[@class='h5']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//a[@id='show-phonenumber-2']/div/strong/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data