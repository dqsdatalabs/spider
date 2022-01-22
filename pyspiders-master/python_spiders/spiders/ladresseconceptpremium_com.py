# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'ladresseconceptpremium_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Ladresseconceptpremium_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "http://www.ladresse-conceptpremium.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "apartment"},
	        {"url": "http://www.ladresse-conceptpremium.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17&C_27_tmp=2&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'col-lg-6')]/div[@class='products-cell']/@data-product-id").extract():
            follow_url = f"http://www.ladresse-conceptpremium.com/catalog/products_print.php?products_id={item}"
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        check_page = response.xpath("//h1[@class='entry-title page-header']/text()[contains(.,'Liste des annonces immobili')]").get()
        if check_page:
            return

        item_loader.add_value("external_source", self.external_source)
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        price = "".join(response.xpath("//div[contains(@class,'prix')]/span/text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0",""))

        meters = "".join(response.xpath("//div[div[. ='Surface']]/div[2]//text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m2")[0].split(".")[0])
        
        utilities = response.xpath("//div[contains(.,'Honoraires')]/following-sibling::div/b/text()").get()
        if utilities:
            utilities = str(int(float(utilities.split(" ")[0].strip())))
            item_loader.add_value("utilities", utilities)

        deposit = "".join(response.xpath("//div[div[. ='Dépôt de Garantie']]/div[2]//text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0])
        
        bathroom_count = response.xpath("//div[contains(.,'Salle')]/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//div[contains(.,'Stationnement')]/following-sibling::div/b/text()").get()
        if parking and "non" not in parking.lower():
            item_loader.add_value("parking", True)
        elif parking:
             item_loader.add_value("parking", False)
            
        furnished = response.xpath("//div[contains(.,'Cuisine')]/following-sibling::div/b/text()").get()
        if furnished and "non" not in furnished.lower():
            item_loader.add_value("furnished", True)
        elif furnished:
             item_loader.add_value("furnished", False)

        available_date = response.xpath(
            "//div[div[. ='Date de disponibilité']]/div[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        external_id = "".join(response.xpath("//div[contains(@class,'reference')]/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        item_loader.add_xpath("room_count", "//div[div[. ='Nombre pièces']]/div[2]//text()")
        item_loader.add_xpath("floor", "//div[div[. ='Etage']]/div[2]//text()")
        item_loader.add_xpath("energy_label", "//div[div[. ='Conso Energ']]/div[2]//text()[.!='Vierge']")
        item_loader.add_xpath("zipcode", "//div[div[. ='Code Postal Internet']]/div[2]//text()")
        item_loader.add_xpath("address", "//div[div[. ='Ville']]/div[2]//text()")
        item_loader.add_xpath("city", "//div[div[. ='Ville']]/div[2]//text()")

        furnished = response.xpath("//div[div[. ='Meublé']]/div[2]//text()[.!='Non']").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[div[. ='Ascenseur']]/div[2]//text()[.!='Non']").get()
        if elevator:
            item_loader.add_value("elevator", True)

        desc = "".join(response.xpath("//div[contains(@class,'description_text')]/text()").extract())
        item_loader.add_value("description", desc.strip())

        if "balcon" in desc:
            item_loader.add_value("balcony", True)

        images = [response.urljoin(x)for x in response.xpath("//ul[@class='bloc_photos']/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "01.64.06.31.32")
        item_loader.add_value("landlord_email", "Vente-guignes@conceptpremium.com")
        item_loader.add_value("landlord_name", "Ladresseconceptpremium")

        yield item_loader.load_item()