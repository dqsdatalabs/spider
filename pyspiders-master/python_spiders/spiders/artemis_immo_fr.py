# -*- coding: utf-8 -*-
# Author: Gabriel Francis
# Team: Sabertooth
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only

class ArtemisImmoFrSpider(scrapy.Spider):
    name = "artemis_immo_fr"
    allowed_domains = ["artemis-immo.fr"]
    start_urls = [
        {
            'url':'http://artemis-immo.fr/search_annonces_immobilieres.php?type_2=on&nature_1=on&localisation=&budget_min=&budget_max=&surface_habitable_min=&superficie_terrain_min=',
            'property_type':'apartment'
        },
        {
            'url':'http://artemis-immo.fr/search_annonces_immobilieres.php?type_2=on&nature_2=on&localisation=&budget_min=&budget_max=&surface_habitable_min=&superficie_terrain_min=',
            'property_type':'house'
        },

    ]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator='.'
    scale_separator=','
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url.get('url'), 
                callback=self.parse,
                meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="bien"]')
        for listing in listings:
            property_url = response.urljoin(listing.xpath('.//div[@class="title"]/a/@href').extract_first())
            yield scrapy.Request(
                url=property_url,
                 callback=self.get_property_details, 
                 meta={'request_url':property_url,
                    'property_type':response.meta.get('property_type')})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("external_id",'.//td[@class="reference"]/span/text()')
        item_loader.add_xpath("title",'.//div[@class="title"]/a/text()')
        item_loader.add_xpath("description",'.//table[@class="annonce_description"]//text()')

        item_loader.add_xpath("images",'.//td[@class="photos"]//img/@src')
        item_loader.add_xpath("rent_string",'.//td[@class="prix"]/span/text()')

        square_meters = response.xpath('.//td[contains(text(),"Surface area")]/text()').extract_first()
        if square_meters:
            item_loader.add_value('square_meters',extract_number_only(square_meters))
        utilities = response.xpath("//table[@class='annonce_description']//text()[contains(.,'/ charge')]").extract_first()
        if utilities:
            item_loader.add_value('utilities',utilities.split("charges")[0].split("+")[-1])
        address = response.xpath("//div[@id='main_panel']/h1/span/text()").extract_first()
        if address:
            item_loader.add_value('address',address.split(" ")[-1].strip())
            item_loader.add_value('city',address.split(" ")[-1].strip())
        
        room_count = response.xpath('.//td[contains(text(),"Number of rooms")]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count',extract_number_only(room_count))
        item_loader.add_value("landlord_name", "ARTEMIS Conseil")
        item_loader.add_value("landlord_phone", "04 76 71 75 15")

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
