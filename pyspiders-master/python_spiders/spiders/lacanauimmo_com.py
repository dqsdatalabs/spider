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
class MySpider(Spider):
    name = 'lacanauimmo_com_disabled'
    start_urls = ['http://www.lacanau-immo.com/location-vacances/1']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='selectionBien bxrow']/article"):
            follow_url = item.xpath(".//a/@href").get()
            property_type = item.xpath(".//h3/text()").get()
            property_type = property_type.strip().split(' ')[0].strip()
            if 'Maison' in property_type or 'Villa' in property_type:
                property_type = 'house'
            elif 'Appartement' in property_type:
                property_type = 'apartment'
            else:
                property_type = 'pass'
            if property_type != 'pass':
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
                seen = True

        if page == 2 or seen:
            url = f"http://www.lacanau-immo.com/location-vacances/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Lacanauimmo_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//h1[@class='titleBien']/text()").get()
        if title:
            title = title.strip()
        item_loader.add_value("title", title)

        rent = response.xpath("//p[@class='price']/text()[not(contains(.,'Nous consulter'))]").get()
        if rent:
            rent = rent.strip().split('€')[0].replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", 'EUR')
        

        item_loader.add_value("external_link", response.url)

        latitude_longitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('center: {')[1].split('}')[0]
            latitude = latitude_longitude.split(',')[0].strip().split(':')[1].strip()
            longitude = latitude_longitude.split(',')[1].strip().split(':')[1].strip()  

            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = "".join(response.xpath("substring-after(//ul[@id='infos']/li[contains(.,'Ville')]/text(),': ')").extract())
        zipcode = "".join(response.xpath("substring-after(//ul[@id='infos']/li[contains(.,'Code postal')]/text(),': ')").extract())
        if address:
            item_loader.add_value("address","{} {}".format(address,zipcode))
            item_loader.add_value("zipcode", zipcode)

            item_loader.add_value("city",address)

        item_loader.add_xpath("floor","substring-after(//ul[@id='infos']/li[contains(.,'Nombre de niveaux')]/text(),': ')")
        item_loader.add_xpath("bathroom_count","substring-after(//ul[@id='details']/li[contains(.,'salle')]/text(),': ')")


        property_type = response.meta.get("property_type")
        if property_type:
            item_loader.add_value("property_type", property_type)

        square_meters = response.xpath("//ul[@id='infos']/li[contains(.,'Surface habitable (m²)')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[1].split('m')[0].strip().replace(',', '.').replace(' ', '.')
            square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))
        else:
            meters = "".join(response.xpath("substring-after(//ul[@id='infos']/li[contains(.,'surface terrain')]/text(),': ')").getall())
            if meters:
                square_meters = meters.split('m²')[0]
                item_loader.add_value("square_meters", str(square_meters))

        room_count = response.xpath("//ul[@id='infos']/li[contains(.,'pièces')]/text()").get()
        if room_count: 
            room_count = room_count.split(':')[1].strip()
            item_loader.add_value("room_count", room_count)

        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
        item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='offreContent']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)


        images = response.xpath("//ul[@class='slider_Mdl']//img/@src").getall()
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        parking = response.xpath("//ul[@id='details']/li[contains(.,'garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        petts = "".join(response.xpath("//div[@class='offreContent']/p/text()[contains(.,'Animaux non souhaités')]").extract())
        if "animaux non souhaités" in petts.lower():
            item_loader.add_value("pets_allowed", False)


        balcony = response.xpath("//ul[@id='details']/li[contains(.,'alcon')]/text()").get()
        if balcony:
            if balcony.split(':')[1].strip().upper() == 'OUI':
                item_loader.add_value("balcony", True)
            if balcony.split(':')[1].strip().upper() == 'NON':
                item_loader.add_value("balcony", False)
        

        terrace = response.xpath("//ul[@id='details']/li[contains(.,'errasse')]/text()").get()
        if terrace:
            if terrace.split(':')[1].strip().upper() == 'OUI':
                item_loader.add_value("terrace", True)
            if terrace.split(':')[1].strip().upper() == 'NON':
                item_loader.add_value("terrace", False)
        

        landlord_phone = response.xpath("//a[@class='dispPhoneAgency']/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", "Lacanauimmo")
        item_loader.add_value("landlord_email", "brussol@orange.fr")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data