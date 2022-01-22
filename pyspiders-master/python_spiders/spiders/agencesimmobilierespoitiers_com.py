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
import math
import dateparser
import re

class MySpider(Spider):
    name = 'agencesimmobilierespoitiers_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencesimmobilierespoitiers_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.agencesimmobilierespoitiers.com/a-louer/appartements/", "property_type": "apartment"},
            {"url": "http://www.agencesimmobilierespoitiers.com/a-louer/studios/1", "property_type": "studio"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        base_url = response.meta.get("base_url")

        seen = False
        for item in response.xpath("//ul[@class='listingUL']/li//h1/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = base_url + f"{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agencesimmobilierespoitiers_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//h1[@itemprop='name']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            external_id = external_id.strip().split(' ')[1]
        item_loader.add_value("external_id", external_id)

        latitude_longtitude = response.xpath("//script[contains(., 'getMap')]/text()").get()
        if latitude_longtitude:
            latitude_longtitude = latitude_longtitude.split('center: {')[1].split('}')[0]
            latitude = latitude_longtitude.split(',')[0].strip().split(':')[1].strip()
            longtitude = latitude_longtitude.split(',')[1].strip().split(':')[1].strip()
        
        address = response.xpath("//p[span[contains(.,'Ville')]]/span[2]/text()").get()
        zipcode = response.xpath("//p[span[contains(.,'Code')]]/span[2]/text()").get()
        
        if address:
            item_loader.add_value("address", address.strip())
        
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        item_loader.add_value("longitude", longtitude)
        item_loader.add_value("latitude", latitude)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//span[.='Surface habitable (m²)']/parent::p/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0].replace(',', '.')
            item_loader.add_value("square_meters", str(math.ceil(float(square_meters))))

        if response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("normalize-space(//span[@class='termInfos' and contains(.,'pièces')]/following-sibling::*/text())").get()
            if room_count:
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)
        

        price = response.xpath("//span[.='Loyer CC* / mois']/parent::p/span[2]/text()").get()
        if price: 
            price = price.split('€')[0].strip()
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        description_text = ""
        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            description_text = " ".join(description)
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        images = [x for x in response.xpath("//ul[@class='imageGallery  loading']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//span[.='Meublé']/parent::p/span[2]/text()").get()
        if furnished:
            if furnished.strip().lower().startswith("non"):
                furnished = False
            else:
                furnished = True
        item_loader.add_value("furnished", furnished)

        floor = response.xpath("//span[.='Etage']/parent::p/span[2]/text()").get()
        if floor:
            floor = floor.strip()
        item_loader.add_value("floor", floor)

        elevator = response.xpath("//span[.='Ascenseur']/parent::p/span[2]/text()").get()
        if elevator:
            if elevator.strip().lower().startswith("non"):
                elevator = False
            else:
                elevator = True
        item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//span[.='Balcon']/parent::p/span[2]/text()").get()
        if balcony:
            if balcony.strip().lower().startswith("non"):
                balcony = False
            else:
                balcony = True
        item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//span[.='Terrasse']/parent::p/span[2]/text()").get()
        if terrace:
            if terrace.strip().lower().startswith("non"):
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        else:
            if "terrasses" in description_text.lower():
                item_loader.add_value("terrace", True)
        

        parking = response.xpath("//span[.='Nombre de parking']/parent::p/span[2]/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
            else:
                parking = False
        item_loader.add_value("parking", parking)

        landlord_email = response.xpath("//ul[@class='coords']/li[2]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
        item_loader.add_value("landlord_email", landlord_email)

        landlord_phone = response.xpath("//ul[@class='coords']/li[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
        item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_name", "Agence Du Palais")

        city = response.xpath("//span[@class='termInfos' and contains(.,'Ville')]/following-sibling::*/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        else:
            item_loader.add_value("city", "Poitiers")

        utilities = response.xpath("normalize-space(//span[@class='termInfos' and contains(.,'charge')]/following-sibling::*/text())").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").strip())

        bathroom_count = response.xpath("normalize-space(//span[@class='termInfos' and contains(.,'salle')]/following-sibling::*/text())").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        if "disponible à partir du" in description_text.lower():
            available_date = description_text.lower().split("partir du")[1].split(".")[0].strip()
            if available_date:
                date_parsed = dateparser.parse(available_date)
                like_date = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", like_date)
        


        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data