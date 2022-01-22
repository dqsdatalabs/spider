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
import math

class MySpider(Spider):
    name = 'agencedelanjou_fr'
    start_urls = ['https://www.agencedelanjou.fr/a-louer/1']  # LEVEL 1
    execution_type= 'testing'
    country= 'france'
    locale= 'fr'
    external_source="Agencedelanjou_PySpider_france_fr"
    thousand_separator = ','
    scale_separator = '.'
    custom_settings = {
        "PROXY_FR_ON" : True,
    }
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='bienTitle']/h1/a"):
            follow_url = response.urljoin(item.xpath("./@href").extract_first())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.agencedelanjou.fr/a-louer/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//div[@class='themTitle']/h1/text()")

        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            external_id = external_id.split(' ')[1].strip()
            item_loader.add_value("external_id", external_id)

        latitude_longtitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longtitude:
            latitude_longtitude = latitude_longtitude.split('center: {')[1].split('}')[0]
            latitude = latitude_longtitude.split(',')[0].strip().split(':')[1].strip()
            longtitude = latitude_longtitude.split(',')[1].strip().split(':')[1].strip()
            
            item_loader.add_value("longitude", longtitude)
            item_loader.add_value("latitude", latitude)

        property_type = response.xpath("//div[@class='bienTitle']/h2/text()").get()
        if property_type:
            property_type = property_type.strip().split('-')[0].split(' ')[0].strip()
            if "aison" in property_type.lower():
                item_loader.add_value("property_type", "house")
            elif "appartement" in property_type.lower():
                item_loader.add_value("property_type", "apartment")

        square_meters = response.xpath("//span[.='Surface habitable (m²)']/parent::p/span[2]/text()").get()
        if square_meters:
            square_meters = math.ceil(float(square_meters.strip().split(' ')[0].replace(",", ".")))
            item_loader.add_value("square_meters", str(square_meters))

        address = "".join(response.xpath("//div[@class='title themTitle elementDtTitle']/h1/text()").extract())
        zipcode = response.xpath("normalize-space(//p[span[.='Code postal']]/span[2]/text())").extract_first()
        if address:
            # item_loader.add_xpath("city",address.split("(")[0].strip().split(" ")[-1])
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("address",address.split("(")[0].strip())
        city= response.xpath("//p[span[contains(.,'Ville')]]/span[2]//text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip())
            
        item_loader.add_xpath("bathroom_count","normalize-space(//p[span[contains(.,'salle' )]]/span[2]/text())")
        item_loader.add_xpath("floor","normalize-space(//p[span[.='Etage']]/span[2]/text())")

        parking = response.xpath("normalize-space(//p[span[contains(.,'parking' )]]/span[2]/text())").get()
        if parking and "non" not in parking.lower():
            item_loader.add_value("parking", True)

        deposit = "".join(response.xpath("//p[span[.='Dépôt de garantie TTC']]/span[2]/text()").extract())
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        utilities = "".join(response.xpath("//p[span[contains(.,'Charges locatives')]]/span[2]/text()").extract())
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        room_count = response.xpath("//span[.='Nombre de chambre(s)']/parent::p/span[2]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count2 = "".join(response.xpath("//span[.='Nombre de pièces']/parent::p/span[2]/text()").extract()).strip()
            item_loader.add_value("room_count", room_count2)


        price = "".join(response.xpath("//ul[@class='list-inline']/li[contains(.,'€')]/text()").extract())
        if price:
            price = price.split("€")[0].strip().replace(",",".").replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        description = response.xpath("//p[@itemprop='description']").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        images = [x for x in response.xpath("//ul[@class='imageGallery  loading']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        balcony = response.xpath("//span[.='Balcon']/parent::p/span[2]/text()").get()
        if balcony:
            if balcony.strip().upper() == 'NON':
                balcony = False
            else:
                balcony = True
            item_loader.add_value("balcony", balcony)

        elevator = "".join(response.xpath("//span[.='Ascenseur']/parent::p/span[2]/text()").getall())
        if elevator:
            if 'NON' in elevator.strip().upper():
                elevator = False
            else:
                elevator = True
            item_loader.add_value("elevator", elevator)

        terrace = "".join(response.xpath("//span[.='Terrasse']/parent::p/span[2]/text()").getall())
        if terrace:
            if 'NON' in terrace.strip().upper():
                terrace = False
            else:
                terrace = True
            item_loader.add_value("terrace", terrace)

        item_loader.add_value("landlord_name", "Agence de Lanjou")

        landlord_email = response.xpath("//ul[@class='coords']/li[3]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        landlord_phone = response.xpath("//ul[@class='coords']/li[1]/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        if item_loader.get_collected_values("property_type"):
            yield item_loader.load_item()


class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data