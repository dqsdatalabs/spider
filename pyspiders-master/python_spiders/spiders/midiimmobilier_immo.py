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
import dateparser
import re
 
class MySpider(Spider): 
    name = 'midiimmobilier_immo'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.midiimmobilier.immo/fr/liste.htm?ope=2&filtre=2#TypeModeListeForm=text&ope=2&filtre=2",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.midiimmobilier.immo/fr/liste.htm?ope=2&filtre=8#TypeModeListeForm=text&ope=2&filtre=8",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page",2)
        
        seen = False
        for item in response.xpath("//span[@class='ico-loupe']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        total_page = int(response.xpath("//span[@class='nav-page-position']/text()").get().split("/")[1].strip())
        
        if page <= total_page:
            if response.meta.get("property_type") == "apartment":
                url = f"https://www.midiimmobilier.immo/ajax/ListeBien.php?page={page}&TypeModeListeForm=text&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0"
                yield Request(url=url,
                                    callback=self.parse,
                                    meta={'property_type': response.meta.get('property_type'), "page": page+1})

        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h2[@class='detail-bien-ville']/text()")
        item_loader.add_value("external_source", "Midiimmobilier_PySpider_"+ self.country + "_" + self.locale)

        latitude = response.xpath("//li[@class='gg-map-marker-lat']/text()").get()
        longitude = response.xpath("//li[@class='gg-map-marker-lng']/text()").get()
        if latitude != "0" and longitude != "0":
            latitude = latitude.strip()
            longitude = longitude.strip()            
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = response.xpath("//div[@class='detail-bien-title']/h2/text()").get()
        if address:
            item_loader.add_value("address", address)
            try:               
                city = address.split("(")[0].strip()
                zipcode = address.split("(")[1].split(")")[0].strip()
                if city:
                    item_loader.add_value("city", city)
                if zipcode:
                    item_loader.add_value("zipcode", zipcode)
            except:
                pass


        square_meters = response.xpath("//span[@class='ico-surface']/parent::li/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters) 
        

        room_count = response.xpath("//ul[@class='nolist']/li[contains(.,'chambre')]/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0].strip()
            if room_count.isnumeric():
                item_loader.add_value("room_count", room_count)
        
        romcheck=item_loader.get_output_value("room_count")
        if not romcheck:
            room_count1 = response.xpath("//ul[@class='nolist']/li[contains(.,'pièce') or contains(.,'pièce(s)')]/text()").get()
            if room_count1:
                room_count1=int(room_count1.strip().split(" ")[0].strip())
                if room_count1:
                   item_loader.add_value("room_count", room_count1)



       

            

        rent = response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
            # rent = rent.split('€')[0].strip()
            # item_loader.add_value("rent", rent)
            # currency = 'EUR'
            # item_loader.add_value("currency", currency)

        external_id = response.xpath("//div[@class='detail-bien-title']/div[2]/span[2]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//span[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        available_date = response.xpath("//span[@itemprop='description']/text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split(':')[1].strip()
            if available_date.isalpha() != True:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@class='scrollpane']//img/@src[not(contains(.,'vide_detail_mini'))]").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Dépôt')]/span[2]/text()").get()
        if deposit:
            deposit = deposit.strip()
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//li[contains(.,'charges')]//span[2]//text()[.!='0']").get()
        if utilities:
            utilities = utilities.strip()
            item_loader.add_value("utilities", utilities.replace(" ",""))
        item_loader.add_value("landlord_name", "MIDI IMMOBILIER")
        item_loader.add_value("landlord_email", "contact@midiimmobilier.immo")
        item_loader.add_value("landlord_phone", "05 63 48 10 10")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data