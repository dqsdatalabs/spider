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

class MySpider(Spider):
    name = 'sud_europe_immo_fr'
    execution_type='testing'
    country='france'
    locale='en'
    custom_setttins = {"HTTPCACHE_ENABLED": False}
    external_source = "Sudeuropeimmo_PySpider_france_en"

    headers = {
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'cache-control': "no-cache",
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'host': "www.sud-europe-immo.fr",
        'origin': "https://www.sud-europe-immo.fr",
        'sec-fetch-dest': "document",
        'sec-fetch-mode': "navigate",
        'sec-fetch-site': "same-origin",
        'sec-fetch-user': "?1",
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
    }

    def start_requests(self):
        start_urls = [
            {
                "nature" : 2,
                "type" : 1,
                "property_type" : "apartment"
            },
            {
                "nature" : 2,
                "type" : 2,
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            
            # r_type = url.get("type")
            # r_nature = url.get("nature")
            #payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"nature\"\r\n\r\n{r_nature}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"type[]\"\r\n\r\n{r_type}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"price\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"age\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"tenant_min\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"tenant_max\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"rent_type\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"newprogram_delivery_at\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"newprogram_delivery_at_display\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"currency\"\r\n\r\nEUR\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"customroute\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"homepage\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"

            yield Request(url="https://www.sud-europe-immo.fr/en/search/",
                                 callback=self.parse,
                                 method="POST",
                                 #body=payload,
                                 headers=self.headers,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[.='Detailed view']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[@title='Next page']/@href").get()
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
        dontallow=response.url
        if dontallow and "sale" in dontallow: 
            return 


        item_loader.add_value("external_source", "Sudeuropeimmo_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('_2 = L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('_2 = L.marker([')[1].split(',')[1].split(']')[0]
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['postcode']:
                        zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            else:
                return
        else: 
            return

        square_meters = response.xpath("//li[contains(.,'Total area')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)
        else:
            return

        room_count = response.xpath("//li[contains(.,'Rooms')]/span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        else:
            return

        rent = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[contains(.,'€')]/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(',', '')
            item_loader.add_value("rent", rent)
        else:
            return

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[contains(.,'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
            item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']//a/@href").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//div[@class='diagnostics details ']/img[contains(@alt,'consumption')]/@src").get()
        if energy_label:
            energy_label = energy_label.split('/')[-1].strip()
            if int(energy_label) <= 50:
                energy_label = 'A'
            elif 50 < int(energy_label) and int(energy_label) <= 90:
                energy_label = 'B'
            elif 90 < int(energy_label) and int(energy_label) <= 150:
                energy_label = 'C'
            elif 150 < int(energy_label) and int(energy_label) <= 230:
                energy_label = 'D'
            elif 230 < int(energy_label) and int(energy_label) <= 330:
                energy_label = 'E'
            elif 330 < int(energy_label) and int(energy_label) <= 450:
                energy_label = 'F'
            elif 450 < int(energy_label):
                energy_label = 'G'
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            floor = floor.split('/')[0].strip().strip('st').strip('nd').strip('rd').strip('th')
            item_loader.add_value("floor", floor)

        parking = response.xpath("//div[@class='areas details']//li[contains(.,'Park')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[contains(.,'Swimming pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_name = response.xpath("//p[@class='smallIcon userName']/strong/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='userBlock']//span[@class='phone smallIcon']/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//div[@class='userBlock']//span[@class='mail smallIcon']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()
