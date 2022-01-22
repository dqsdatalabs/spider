# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'erlea_fr' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Erlea_PySpider_france"
    def start_requests(self):

        prop = ["Appartement","Maison"]
        for p in prop:
            formdata = {
                "z": "All",
                "v": "All",
                "m": f"{p}",
                "ch": "All",
                "ref": "",
            }
            yield FormRequest(
                url="https://www.erlea.fr/fr/locations.htm",
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":p,
                }
            )
            
    # 2. SCRAPING level 2
    def parse(self, response):
      
        for item in response.xpath("//div[@class='property-thumb-info']/div/a"):
            url = response.urljoin(item.xpath("./@href").extract_first())
            yield Request(url,callback=self.populate_item , meta={"property_type": response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "Appartement" in response.meta.get('property_type'):
            item_loader.add_value("property_type","apartment")
        elif "Maison" in response.meta.get('property_type'):
            item_loader.add_value("property_type","house")
            
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("city", "normalize-space(//ul[contains(@class,'list-unstyled')]/li/address/text())")

        item_loader.add_xpath("address", "//div[@class='col-md-4 sidebar']/strong/text()")
        item_loader.add_xpath("zipcode", "substring-after(//div[@class='col-md-4 sidebar']/strong/text(),', ')")
        item_loader.add_xpath("square_meters", "normalize-space(//ul[contains(@class,'list-unstyled')]/li/strong[.='Surface:']/following-sibling::text())")
        item_loader.add_xpath("room_count", "normalize-space(//ul[contains(@class,'list-unstyled')]/li/strong[.='Pièces:']/following-sibling::text())")

        item_loader.add_xpath("bathroom_count", "normalize-space(//ul[contains(@class,'list-unstyled')]/li/strong[.='Salles de bains:']/following-sibling::text())")
        item_loader.add_xpath("energy_label", "substring-after(//div[@id='dpe']/span/@class,'_')")
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),','),')')")


        external_id = "".join(response.xpath("//ul[contains(@class,'list-unstyled')]/li/strong[.='Réf:']/following-sibling::text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        deposit = "".join(response.xpath("normalize-space(//ul[contains(@class,'list-unstyled')]/li/strong[.='Dépôt de garantie:']/following-sibling::text())").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())    

        utilities = "".join(response.xpath("normalize-space(//ul[contains(@class,'list-unstyled')]/li/strong[.='Charges:']/following-sibling::text())").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.strip()) 

        description = " ".join(response.xpath("//div[@class='tdpdetail']/div//text()").getall())   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='flexslider']//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        rent = response.xpath("substring-after(//h3[@class='st']/text()[contains(.,'Loyer')],': ')").get()
        if rent:
            rent=rent.replace(" ","")
            item_loader.add_value("rent_string",rent.strip())       

        furnished = "".join(response.xpath("//ul[contains(@class,'list-unstyled')]/li/strong[.='Meublé:']/following-sibling::text()").extract())
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "ERLEA IMMOBILIER")
        item_loader.add_value("landlord_phone", "05 59 37 07 91")
        item_loader.add_value("landlord_email", "contact@erleaimmo.com")

        yield item_loader.load_item()