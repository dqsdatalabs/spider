# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
import dateparser

class PartenaireImmobilierSpider(scrapy.Spider):
    name = "partenaire_immobilier"
    allowed_domains = ["www.partenaire-immobilier.be"]
    start_urls = (
        'http://www.www.partenaire-immobilier.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'
    external_source="Partenaire_immobilier_PySpider_belgium_nl"

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.partenaire-immobilier.be/nos-biens/?filter-property-type=15&filter-contract=RENT', 'property_type': 'apartment'},
            {'url': 'https://www.partenaire-immobilier.be/nos-biens/?filter-property-type=36&filter-contract=RENT', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//article')
        for link in links: 
            link_validation = response.xpath('.//div//span[contains(@class, "property-row-meta-item-status")]/strong/text()').extract_first('').strip()
            if 'loué' in link_validation.lower():
                continue  
            url = response.urljoin(link.xpath('./a/@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[@class="next page-numbers"]/@href'):
            next_link = response.urljoin(response.xpath('//a[@class="next page-numbers"]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response): 
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//div[@class='property-gallery-preview']/span/text()[.='LOUÉ']").extract_first()
        if rented:return
            
        external_link = response.url

        property_type = response.meta.get('property_type')
        address = response.xpath('//span[contains(text(), "Adresse")]/following-sibling::strong/text()').extract_first('').strip()
        city = " ".join(response.xpath("//div[@class='property-overview']/ul/li/span[.='Situation']/following-sibling::strong//text()").extract()).strip()
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("p=")[-1])
        rent_string = response.xpath('//span[contains(text(), "Prix")]/following-sibling::strong/text()').extract_first('').strip()
        rent = re.sub(r'[\s]+', '', rent_string)
        square_meters = response.xpath('//span[contains(text(), "Surface")]/following-sibling::strong/text()').extract_first('').strip().split('m2')[0]
        room_count = response.xpath('//span[contains(text(),"Chambres")]/following-sibling::strong/text()').extract_first('').strip()
        bathrooms = response.xpath('//span[contains(text(),"Salle(s)")]/following-sibling::strong/text()').extract_first('').strip() 
        if room_count and square_meters:   
            
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_xpath('title', '//h1[contains(@class, "property-title")]/text()')
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_xpath('description', '//div[@class="property-description"]/p/text()')
            item_loader.add_value('rent_string', rent)
            item_loader.add_xpath('images', '//div[@class="property-gallery-preview"]/a/@href')
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('square_meters', str(square_meters))
            item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Partenaire Immobilier')
            item_loader.add_value('landlord_email', 'contact@partenaire-immobilier.be')
            available_date=response.xpath("//div[@class='property-overview']/ul/li/span[.='Disponibilité']/following-sibling::strong//text()[not(contains(.,'Disponible de suite'))]").get()

            if available_date:
                date2 =  available_date.replace("Le","").strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

            utilities =  response.xpath("//div[@class='property-overview']/ul/li//span[.='Charges mensuelles']/following-sibling::strong/text()").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities)

            zipcode = response.xpath("//div[@class='property-map-position']/strong/text()").extract_first()
            if zipcode:
                if "à" in zipcode:
                    zipcode = zipcode.split("à")[1].strip().split(" ")[0].strip()
                    item_loader.add_value("zipcode", zipcode)
            item_loader.add_xpath("latitude", "//div[@class='map']/@data-latitude")
            item_loader.add_xpath("longitude", "//div[@class='map']/@data-longitude")

            parking =  response.xpath("//div[@class='property-overview']/ul/li/span[.='Garages']/following-sibling::strong//text()").extract_first()
            if parking:
                item_loader.add_value("parking", True)

            furnished =  response.xpath("//ul/li[@class='yes']/text()[contains(.,'meublé')]").extract_first()
            if furnished:
                item_loader.add_value("furnished", True)
            elif response.xpath("//ul/li[@class='no']/text()[contains(.,'meublé')]").extract_first():
                item_loader.add_value("furnished", False)

            item_loader.add_value('landlord_phone', '+32 4 224 02 22')
            item_loader.add_value("external_source", self.external_source)
            yield item_loader.load_item()