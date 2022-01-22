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

class MySpider(Spider):
    name = 'mdi44_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    start_urls = ["http://www.reseau-mdi.com/annonces/location/"]

    def parse(self,response):

        items = response.xpath("//select[@class='form-control']/option[contains(.,'APPARTEMENT')]/@value").extract_first()
        formdata = {
            "familly": "1",
            "basefamilly": "1",
            "familly": "3",
            "_nature_bien_1469004425_@!@0": f"{items}",
            "212!140!12!505!143!13@!@0": "",
            "12!140!12!505!143!13@!@0_custom": "",
            "_nombre_de_chambres_1369400957_@!@0": "",
            "_surface_habitable_1369401084_@!@0": "",
            "minPrice": "",
            "maxPrice": "",
            "_nombre_de_piÈces_1469004425_@!@0": "",
            "rayon": "",
            "reference": "",
            "_nom_agence_1476780615_@!@0": "",
            "_departement_1472824875_@!@0":"" ,
            "search": "rechercher",
            "engine": "1",
            "orand": "",
        }

        yield FormRequest(
            url="http://www.reseau-mdi.com/annonces/location/",
            callback=self.parse_list,
            formdata=formdata,
            meta={"property_type": "apartment"}
    )
 
    # 1. FOLLOWING
    def parse_list(self, response):

        for item in response.xpath("//div[@class='row']/div[contains(@class,'st-prod')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "MAISON" in response.meta.get('property_type'):
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", "apartment")

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mdi44_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@id='reference']/text(),':  ')")

        rent = "".join(response.xpath("//div[@class='prod--left-price']/div/text()").extract())
        if rent:
            price = rent.replace("\xa0","").replace(",","").replace(" ","").strip()
            item_loader.add_value("rent_string", price.strip())
        item_loader.add_xpath("utilities", "substring-after(//div[@class='prod--left-price']/div/span/text()[contains(.,'les charges')],': ')")

        address = " ".join(response.xpath("//div[@class='prod--left-details']/span/text()").extract())
        if address:
            address = address.replace("-"," ")
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        desc = " ".join(response.xpath("//div[@class='prod--right-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images = [response.urljoin(x) for x in response.xpath("//div[@id='prod--slider']/ul/li/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        meters = " ".join(response.xpath("//ul[@class='prod--right-property clearfix list-unstyled']/li[span[.='Surface habitable']]/span[2]/text()").extract())
        if meters:
            s_meters = meters.split("m²")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters)))

        item_loader.add_xpath("room_count", "//ul[@class='prod--right-property clearfix list-unstyled']/li[span[.='Nombre de chambres']]/span[2]/text()")
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            item_loader.add_xpath("room_count", "//ul[@class='prod--right-property clearfix list-unstyled']/li[span[.='Nombre de pièces']]/span[2]/text()")
            
 

        item_loader.add_xpath("bathroom_count", "//ul[@class='prod--right-property clearfix list-unstyled']/li[span[contains(.,'salle')]]/span[2]/text()[.!='0']")
        item_loader.add_xpath("deposit", "//ul[@class='prod--right-property clearfix list-unstyled']/li[span[.='Dépôt de garantie']]/span[2]/text()")
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//figure[@class='sr-only print-visible']/img/@src[contains(.,'dpe')],'dpe/'),'.')")
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'map.setPoint')],'map.setPoint('),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'map.setPoint')],'map.setPoint('),', '),')')")

        item_loader.add_value("landlord_phone", "04 50 74 32 50")
        item_loader.add_value("landlord_name", "MDI 74 IMMOBILIER")  

        yield item_loader.load_item()
 