# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader

class AdhocimmoBeSpider(scrapy.Spider):
    name = "adhocimmo_be"
    allowed_domains = ["www.adhocimmo.be"]
    start_urls = (
        'http://www.www.adhocimmo.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [{'url':'http://www.adhocimmo.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Appartement&chambre_min=&prix_max=&clangue=fr',
            'property_type':'apartment'},
            {'url':'http://www.adhocimmo.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Maison&chambre_min=&prix_max=&clangue=fr',
            'property_type':'house'},]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), 
                callback=self.parse,
                meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"portfolio-item")]')
        for listing in listings:
            property_url = listing.xpath('.//div/a/@href').extract_first()
            property_url = response.urljoin(property_url)

            room_count = listing.xpath('.//span[@class="chambre"]/text()').extract_first()
            bathroom_count = listing.xpath('.//span[@class="sdb"]/text()').extract_first()
            parking_check = listing.xpath('.//span[@class="garage"]/text()').extract_first()
            if parking_check:
                parking = True
            else:
                parking = False
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={'request_url':property_url,
                    'property_type':response.meta.get("property_type"),
                    'room_count':room_count,
                    'bathroom_count':bathroom_count,
                    'parking':parking})

        next_page_url = response.xpath('.//a[contains(text(),"Suivant >")]/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(
                url=next_page_url, 
                callback=self.parse, 
                meta={'property_type':response.meta.get("property_type")})


    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))

        external_id = response.xpath('.//p[contains(text(),"Réf")]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id',external_id.split(':')[-1])

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("room_count", response.meta.get('room_count'))
        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            bathroom_count = response.xpath("//ul[@class='check_list']/li/text()[contains(.,'de douche') and not(contains(.,'lavabo'))]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        item_loader.add_value("parking", response.meta.get('parking'))

        item_loader.add_xpath("square_meters", './/li[contains(text(),"Surface habitable")]/text()')

        if response.xpath("//li/b[contains(text(),'Prix')]/text()[contains(.,'Loué')]").get(): return
        if response.xpath("//div[@id='desc']/p/text()[contains(.,'salle de bain')]").get() and not item_loader.get_collected_values("bathroom_count"): 
            item_loader.add_xpath("bathroom_count", "1")
            
        item_loader.add_xpath("rent_string", './/li/b[contains(text(),"Prix")]/text()')
        item_loader.add_xpath("utilities", './/li[contains(text(),"Charges")]/text()')
        item_loader.add_xpath("title", './/meta[@property="og:title"]/@content')
        city = response.xpath('.//h2/span/text()').extract_first()
        if city:
            city = city[3:].split()[0]
            item_loader.add_value("city", city)
        address = response.xpath('//h2/span/text()').extract_first()
        if address:
            item_loader.add_value("address", address.replace(" - ",""))
        item_loader.add_xpath("description", './/div[@id="desc"]/p/text()')
        item_loader.add_xpath("energy_label", './/div[@id="dpe"]//strong[contains(text(),"Consommation énergétique:")]/following-sibling::text()')

        images = response.xpath('.//div[@id="sliderx"]//img/@src').extract()
        images = [response.urljoin(image) for image in images]
        item_loader.add_value("images", images)

        features = ' '.join(response.xpath('.//div[@id="details"]//li/text()').extract())

        # http://www.adhocimmo.be/fr/annonces-immobilieres/location-non-meublee/schaerbeek/appartement/2176/quartier-europeen-entre-dailly-et-place-des-chasse.html
        if 'ascenseur' in features.lower():
            item_loader.add_value('elevator', True)

        # http://www.adhocimmo.be/fr/annonces-immobilieres/location-non-meublee/woluwe-saint-lambert/maison/2129/avenue-de-mai-superbe-maison-de--200-m--4-chambres.html
        if 'lave vaisselle' in features.lower():
            item_loader.add_value('dishwasher', True)

        # http://www.adhocimmo.be/fr/annonces-immobilieres/location-non-meublee/brussel-1/maison/2169/quartier-europeen-square-ambiorix-tres-agreable-ma.html
        if 'lave linge' in features.lower():
            item_loader.add_value('washing_machine', True)

        # http://www.adhocimmo.be/fr/annonces-immobilieres/location-non-meublee/woluwe-saint-lambert/maison/2129/avenue-de-mai-superbe-maison-de--200-m--4-chambres.html
        if 'balcon' in features.lower():
            item_loader.add_value('balcony', True)

        # http://www.adhocimmo.be/fr/annonces-immobilieres/location-non-meublee/woluwe-saint-lambert/maison/2129/avenue-de-mai-superbe-maison-de--200-m--4-chambres.html
        if 'terrasse' in features.lower():
            item_loader.add_value('terrace',True)
            
        if response.xpath('.//h3[contains(text(),"non meublée")]').extract_first():
            item_loader.add_value('furnished', False)
        elif response.xpath('.//h3[contains(text(), "meublée")]').extract_first():
            item_loader.add_value('furnished', True)

        item_loader.add_value('landlord_name', 'Adhoc Immo')
        item_loader.add_value('landlord_phone', '02/280.69.40')

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
