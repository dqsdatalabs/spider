# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re 
from ..loaders import ListingLoader
from python_spiders.helper import string_found
import dateparser
class LagenceimmobiliereSpider(scrapy.Spider):
    name = "lagenceimmobiliere"
    allowed_domains = ["www.lagenceimmobiliere.be"]
    start_urls = (
        'http://www.www.lagenceimmobiliere.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.lagenceimmobiliere.be/Rechercher/APPARTEMENT%20Locations%20/Locations/Type-03%7CAPPARTEMENT/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE', 'property_type': 'apartment'},
            # {'url': 'https://www.lagenceimmobiliere.be/Rechercher/MAISONS%20Ventes%20/Ventes/Type-01%7CMAISONS/Localisation-/Prix-/Tri-PRIX%20DESC,COMM%20ASC,CODE', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class,"list-item")]/a/@href[not(contains(.,"javascript"))]').getall()
        for link in links:
            url = response.urljoin(link)
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        externalid=response.xpath("//span[contains(.,'Ref')]/b/text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        zipcode = response.xpath("//iframe/@src[contains(.,'maps/')]").get()
        if zipcode:
            zipcode = zipcode.split("q=")[1].split(" ")[0]
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = "".join(response.xpath("//i[contains(@class,'arrow')]//parent::div[contains(.,'Surface')]//text()").getall())
        if square_meters:
            square_meters = square_meters.split(":")[-1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
            
        rent = response.xpath("//td[contains(.,'Price')]//following-sibling::td//text()").get()
        if rent and "€" in rent:
            rent = rent.split("€")[0].replace(".","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        utilities="".join(response.xpath("//div[@class='col-md-6' and contains(@h2,'')]/p//text()").getall())
        if utilities:
            utilities=utilities.split("charges")[-1].split("Charges")[-1].split("eur")[0].split("€")[0]

            item_loader.add_value("utilities",utilities.strip())
        item_loader.add_xpath('description', "//div[@class='col-md-6' and contains(@h2,'')]/p//text()")
        item_loader.add_xpath('images', "//div[@id='carousel']//@src")
        # item_loader.add_xpath('room_count', "//tr[td[contains(.,'Chambre')]]/td[2]/text()")
        item_loader.add_value('landlord_name', "L'Agence Immobilière sprl")
        item_loader.add_value('landlord_email', 'info@lagenceimmobiliere.be')
        item_loader.add_value('landlord_phone', '02/736.10.16')
        room_count=response.xpath("//div[@class='col-xs-4']//br/following-sibling::text()").get()
        if room_count:
            room=re.findall("\d+",room_count)
            item_loader.add_value("room_count",room)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            item_loader.add_xpath('room_count', "//tr[td[contains(.,'Chambre')]]/td[2]/text()")


        floor = response.xpath("//td[contains(.,'étage')]//following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        bathroom_count = response.xpath("//td[contains(.,'Bathroom')]//following-sibling::td//text()").extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)
        terrace = response.xpath("//td[contains(.,'Terrace')]//following-sibling::td//text()[contains(.,'Yes')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        
        energy_label = response.xpath("//tr[td[contains(.,'Espec')]]/td[2]/text()").get()
        
        
        parking = response.xpath("//td[contains(.,'Parking')]//following-sibling::td//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        # available_date = response.xpath('//li//div[contains(text(), "Availability")]/following-sibling::div/text()').extract_first()
        # if available_date:
        #     date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"], languages=['fr'])
        #     if date_parsed:
        #         item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))

        if response.xpath("//td[contains(.,'Furnished')]//following-sibling::td//text()[contains(.,'Yes')]").get(): item_loader.add_value("furnished", True)

        yield item_loader.load_item()