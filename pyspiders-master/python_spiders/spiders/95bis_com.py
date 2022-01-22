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
from urllib.parse import urljoin

class MySpider(Spider):
    name = '95bis_com'
    execution_type='testing' 
    country='france'
    locale='fr' # LEVEL 1 
    scale_separator='.'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):
        yield Request("http://www.95bis.com/a-louer-non-meuble/1", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//div[contains(@class,'listing')]//div[contains(@class,'bien')]"):
            seen = True
            follow_url = response.urljoin(item.xpath("./article/div/a/@href").get())
            property_type = item.xpath(".//h1/following-sibling::p[1]/text()[1]").get()
            if property_type:
                status = item.xpath(".//span[contains(@class,'StatutBien')]/text()").get()
                if status:
                    if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
        if page == 2 or seen:
            url = f"http://www.95bis.com/a-louer-non-meuble/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_xpath("title", "//h1[@class='detail-title']/span/text()")
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("external_source", "95bis_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'getMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat : ')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:  ')[1].split('}')[0].strip()

            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = response.xpath("//th[contains(.,'Ville')]/following-sibling::th/text()").get()
        zipcode = response.xpath("//th[contains(.,'Code')]/following-sibling::th/text()").extract_first()
        if address:
            item_loader.add_value("address", "{} {}".format(address,zipcode))
            item_loader.add_value("city", address)
            item_loader.add_value("zipcode", zipcode)

        item_loader.add_xpath("bathroom_count", "//tr[th[.='Nb de salle de bains']]/th[2]/text()")

        square_meters = response.xpath("//th[contains(.,'Surface habitable (m²)')]/following-sibling::th/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//th[contains(.,'Nombre de pièces')]/following-sibling::th/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//th[contains(.,'Loyer')]/following-sibling::th/text()").get()
        if rent:
            rent = rent.strip().replace(' ', '').replace(".","")
            item_loader.add_value("rent_string", rent)

        utilities = response.xpath("substring-before(//tr[th[contains(.,'charge locataire')]]/th[2]/text(),'€')").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        else:
            utilities2 = response.xpath("substring-before(//tr[th[contains(.,'Charges locatives')]]/th[2]/text(),'€')").get()
            if utilities2:
                item_loader.add_value("utilities", utilities2.replace(" ",""))

        external_id = response.xpath("//b[contains(.,'Ref')]/parent::span/following-sibling::text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = ['http:' + x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//th[contains(.,'Dépôt de garantie')]/following-sibling::th/text()[.!='0' and .!='Non renseigné']").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '')
            item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//th[contains(.,'Meublé')]/following-sibling::th/text()").get()
        if furnished:
            if furnished.strip().lower() == 'non':
                furnished = False
            elif furnished.strip().lower() == 'oui':
                furnished = True
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        floor = response.xpath("//th[contains(.,'Etage')]/following-sibling::th/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//th[contains(.,'Nombre de garage')]/following-sibling::th/text()").get()
        if parking:
            if int(parking) > 0: item_loader.add_value("parking", True)
            elif int(parking) == 0: item_loader.add_value("parking", False)

        elevator = response.xpath("//th[contains(.,'Ascenseur')]/following-sibling::th/text()").get()
        if elevator:
            if elevator.strip().lower() == 'non':
                elevator = False
            elif elevator.strip().lower() == 'oui':
                elevator = True
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//th[contains(.,'Balcon')]/following-sibling::th/text()").get()
        if balcony:
            if balcony.strip().lower() == 'non':
                balcony = False
            elif balcony.strip().lower() == 'oui':
                balcony = True
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//th[contains(.,'Terrasse')]/following-sibling::th/text()").get()
        if terrace:
            if terrace.strip().lower() == 'non':
                terrace = False
            elif terrace.strip().lower() == 'oui':
                terrace = True
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//div[@class='media-body']/span[1]/text()").get()
        if landlord_name and landlord_name != " ":
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "95BIS IMMOBILIER")
            
        landlord_phone = response.xpath("//div[@class='media-body']/span[2]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "04 26 65 57 75")
                    
        landlord_email = response.xpath("//div[@class='media-body']/span[3]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartement" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or 'propriete' in p_type_string.lower()):
        return "house"
    else:
        return None

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data