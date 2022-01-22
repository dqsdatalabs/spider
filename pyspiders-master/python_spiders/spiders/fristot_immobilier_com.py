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
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'fristot_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.fristot-immobilier.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=2&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&annlistepg=1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.fristot-immobilier.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&annlistepg=1",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='row-fluid hidden-desktop hidden-tablet']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True

        if page == 2 or seen:
            p_url = response.url.split("annlistepg")[0] + f"annlistepg={page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type"), "page":page+1},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Fristotimmobilier_PySpider_"+ self.country + "_" + self.locale)
        title="".join(response.xpath("//div//h1//text()").extract())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title",title.strip())
        address="".join(response.xpath("//div//h1//text()[last()]").extract())
        if address:
            address = re.sub("\s{2,}", " ", address)
            item_loader.add_value("address",address.strip())
            item_loader.add_value("city",address.split("(")[0].strip())
            item_loader.add_value("zipcode",address.split("(")[-1].split(")")[0].strip())
        latitude_longitude = response.xpath("//script[contains(.,'LATITUDE')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LATITUDE_CARTO: "')[1].split('"')[0].strip().replace(',', '.')
            longitude = latitude_longitude.split('LONGITUDE_CARTO: "')[1].split('"')[0].strip().replace(',', '.')

            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        square_meters = response.xpath("//div[.='Surface']/following-sibling::div/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li/div[contains(.,'Chambre')]/following-sibling::div/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li/div[contains(.,'Pièce')]/following-sibling::div/text()")
        bathroom_count = response.xpath("//li/div[contains(.,'Salle d')]/following-sibling::div/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(' ')[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//span[contains(.,'Référence')]/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//div[@id='slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(':')[1].split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            if deposit != 'N/A':
                item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//li[contains(.,'Charges :')]/text()").get()
        if utilities:
            utilities = utilities.split(':')[1].split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            if utilities != 'N/A':
                item_loader.add_value("utilities", utilities)
        else:
            utilities = response.xpath("//p[@class='prix-honoraires-charges']//text()[contains(.,'Honoraires :')]").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split(":")[1])
        energy_label = response.xpath("//img[contains(@src,'diag_dpe')]/following-sibling::p[contains(@class,'diagLettre diag')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            if energy_label != 'VI':
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//div[.='Etage']/following-sibling::div/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//div[.='Ascenseur']/following-sibling::div/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)
        furnished = response.xpath("//li/div[contains(.,'Meublé')]/following-sibling::div/text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                furnished = True
            elif furnished.strip().lower() == 'non':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)
        balcony = response.xpath("//div[.='Balcon']/following-sibling::div/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//div[.='Terrasse']/following-sibling::div/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//div[@id='detail-agence-nom']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='margin-top-10']//span[@id='numero-telephonez-nous-detail']/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email","agence.fristot@wanadoo.fr")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data