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
import dateparser

class MySpider(Spider):
    name = 'cabinetfolliot_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.cabinetfolliot.com/catalog/advanced_search_result.php?action=update_search&map_polygone=&latlngsearch=&search_id=1680420423049335&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=", "property_type": "apartment"},
	        {"url": "https://www.cabinetfolliot.com/catalog/advanced_search_result.php?action=update_search&map_polygone=&latlngsearch=&search_id=1680420423049335&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })
    # 1. FOLLOWING
    def parse(self, response):
        #page = response.meta.get('page', 2)
        
        seen = False
        for follow_url in response.xpath("//div[@class='product-cell-slider']/div[2]/div[1]/a/@href").extract():
            yield Request(urljoin('https://www.cabinetfolliot.com', follow_url.lstrip('..')), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        pagination = response.xpath("//ul[@class='pagination']/li/a[@class='page_suivante']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-8 col-lg-8']/h1/text()").get()
        item_loader.add_value("title", title.strip())
        item_loader.add_value("external_source", "Cabinetfolliot_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        meters = response.xpath("//div[@class='info-critere'][contains(.,'m²')][not(contains(.,'m² terrain'))]/div[@class='value']//text()").extract_first()
        if meters:
            item_loader.add_value("square_meters",meters)
        else:
            s_meters = "".join(response.xpath("//div/h1/text()[contains(.,'m²')]").extract())
            if s_meters:
                meters = s_meters.split("m²")[0].strip().split(" ")[-1]
                item_loader.add_value("square_meters",meters)
                
        item_loader.add_xpath("room_count", "//div[@class='info-critere'][contains(.,'pièce') or contains(.,'chambre')]/div[@class='value']//text()")
        item_loader.add_xpath("bathroom_count", "//ul[@class='list-criteres']//li[div[.='Salle(s) de bains']]/div[2]/text()")
        item_loader.add_xpath("city", "//ul[@class='list-criteres']/li[contains(.,'Ville')]/div[@class='value']//text()")
        
        rent = "".join(response.xpath("//div[@class='info-critere']//span[@class='alur_loyer_price']//text()").extract())
        if rent:
            rent = str(int(float(rent.split('€')[0].strip().strip('Loyer').strip())))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')


        address = "".join(response.xpath("//ul[@class='list-criteres']/li[contains(.,'Ville')]/div[@class='value']//text()").extract())
        zipcode = "".join(response.xpath("//ul[@class='list-criteres']/li[contains(.,'Postal')]/div[@class='value']//text()").extract())
        if address:
            address = "{} {}".format(address,zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)


        available_date=response.xpath("//ul[@class='list-criteres']//li[div[.='Disponibilité']]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        elif not available_date:
            
            available_date=response.xpath("//div[@class='desc-product']/text()[contains(.,'Disponible le')]").get()
            print(" available date   ",available_date," ",response.url)
            try:
                if available_date:
                    date_parsed = dateparser.parse(
                        available_date.split("Disponible le")[1].strip(), languages=['fr']
                    )
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)
            except:
                pass

        utilities = "".join(response.xpath("//ul[@class='list-criteres']//li[div[.='Honoraires Locataire']]/div[2]/text()").extract())
        if utilities:
            # utilities = str(int(float(rent.split('€')[0].strip().strip('Loyer').strip())))
            item_loader.add_value("utilities", utilities)

        desc = "".join(response.xpath("//div[@class='desc-product']/text()[not(contains(@class,'mentions_bareme_product'))]").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
        
        
        deposit = "".join(response.xpath("//ul[@class='list-criteres']/li[contains(.,'de Garantie')]/div[@class='value']//text()").extract())
        if deposit :
            item_loader.add_value("deposit", deposit.split("€")[0])


        images = [response.urljoin(x)for x in response.xpath("//div[@class='container-slider-bien']//img/@src").extract()]
        if images:
                item_loader.add_value("images", list(set(images)))


        external_id = "".join(response.xpath("//li[@itemprop='itemListElement']/span/text()").extract())
        if external_id :
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

          

        floor = response.xpath("//ul[@class='list-criteres']/li[contains(.,'Etage')]/div[@class='value']//text()[.!='Non']").extract_first()
        if floor :
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//ul[@class='list-criteres']/li[contains(.,'Ascenseur')]/div[@class='value']//text()").extract_first()
        if elevator :
            if elevator == 'Non':
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        furnished = response.xpath("//ul[@class='list-criteres']/li[contains(.,'Meublé')]/div[@class='value']//text()").extract_first()
        if furnished :
            if furnished == 'Non':
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)



        energy_label=response.xpath("//ul[@class='list-criteres']/li[contains(.,'Conso Energ')]/div[@class='value']//text()[.!='Vierge' and .!='Non communiqué']").extract_first()

        item_loader.add_value("energy_label", energy_label)
        name=response.xpath("//div[@class='nego-name']//text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        elif not name:
            item_loader.add_value("landlord_name","Cabinet FOLLIOT")
   
        phone=response.xpath("//div[@class='nego-contact']/a[1]/@href").extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("tel:")[1])
        elif not phone:
            item_loader.add_value("landlord_phone"," 02.33.50.95.95")
        mail=response.xpath("//div[@class='nego-contact']/a[2]/@href").extract_first()
        if mail:
            item_loader.add_value("landlord_email",mail.split("mailto:")[1])
        elif not mail:
            item_loader.add_value("landlord_email","folliot.avranches@gmail.com")

        yield item_loader.load_item()