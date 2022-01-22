# -*- coding: utf-8 -*-
# Author: Valeriy Nikitiuk
import scrapy
from ..loaders import ListingLoader
import re
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode = zip_city.split("-")[0]
    zipcode_re = zipcode + '-' 
    city = zip_city.replace(zipcode, '') 
    return zipcode, city

class LaproprieteSpider(scrapy.Spider):
    name = 'lapropriete'
    allowed_domains = ['lapropriete']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.lapropriete.be/fr/residentiel/louer-bien-immobilier/appartement',
                'property_type': 'apartment'},
            {'url': 'https://www.lapropriete.be/fr/residentiel/louer-bien-immobilier/maison',
                'property_type': 'house'},
            {'url': 'https://www.lapropriete.be/fr/residentiel/louer-bien-immobilier/flat',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for link in response.xpath('//div[contains(@class, "liste_biens")]//a'):
            property_url = response.urljoin(link.xpath('./@href').extract_first())
            price_text = ''.join(link.xpath('.//p[@class="price"]/text()').extract())
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type'), 'rent': price_text},
                dont_filter=True
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        property_type = response.meta.get('property_type')
        external_id = response.xpath('//p[contains(text(), "Référence ")]/b/text()').extract_first('')
        if external_id:
            address_text = response.xpath('//b[contains(text(), "postal")]/../../following-sibling::td/text()').extract_first('')
            if not address_text:
                address_text = '6000-Charleroi'
            address_street = response.xpath('//b[contains(text(), "Adresse")]/../../following-sibling::td/text()').extract_first('')
            if not address_street:
                address_street = 'Boulevard Audent 17'    
            else:
                address_street = re.findall(r'[A-Za-z\s0-9]+', address_street)[0]
            address = address_street + ', ' + address_text 
            zipcode, city = extract_city_zipcode(address)
            title = ''.join(response.xpath('//title/text()').extract())
            title = re.sub(r'[\r\t\n]+', '', title)

            bathroom_count = response.xpath("//td[contains(.,'de bain')]/following-sibling::td/text()").get()
            if bathroom_count: item_loader.add_value('bathroom_count', bathroom_count.strip())

            if response.xpath("//div[contains(@class,'content-intro')]/h1/text()[contains(.,'meublé')]").get(): item_loader.add_value('furnished', True)

            details_text = ''.join(response.xpath('//div[@class="bien__content"]//p[2]//text()').extract())
            images = []
            image_links = response.xpath('//div[contains(@class, "slide")]//a[@class="bien__img"]')
            for image_link in image_links:
                image_url = image_link.xpath('./@href').extract_first()
                if image_url not in images:
                    images.append(image_url)
            room_count_text = response.xpath('//b[contains(text(), "chambres")]/../../following-sibling::td/text()').extract_first('')
            square_meters_text = response.xpath('//b[contains(text(), "Superficie")]/../../following-sibling::td/text()').extract_first('')
            if square_meters_text: 
                square_meters_re = re.findall(r'\d+', square_meters_text)[0]
                if int(square_meters_re) > 0:
                    square_meters = True
                else:
                    square_meters = ''
            else:
                square_meters = ''
            if 'terrasse' in details_text.lower():
                terrace = True
            else:
                terrace = ''
            parking_text = response.xpath("//table[@class='table table-striped']//h3[.='Garage(s)']/parent::td/../td/text()").get()

            if parking_text and int(parking_text) > 0:
                parking = True
            else:
                parking = ''
            location=response.xpath("//script[contains(.,'routeFrom')]/text()").get()
            if location:
                latitude=location.split("my_lat =")[-1].split(";")[0]
                item_loader.add_value("latitude",latitude)
                longitude=location.split("my_long =")[-1].split(";")[0]
                item_loader.add_value("longitude",longitude)
            if room_count_text and square_meters: 
                
                item_loader.add_value('property_type', property_type)
                item_loader.add_value('external_id', external_id)
                item_loader.add_value('title', title)
                item_loader.add_value('external_link', external_link)
                item_loader.add_value('address', address)
                item_loader.add_xpath('rent_string', '//b[contains(text(), "Loyer")]/../../following-sibling::td/text()')
                item_loader.add_xpath('description', '//div[@class="bien__content"]//p[2]//text()')
                item_loader.add_xpath('square_meters', '//b[contains(text(), "Superficie")]/../../following-sibling::td/text()')
                item_loader.add_value('images', images)
                if parking:
                    item_loader.add_value('parking', True)
                if terrace:
                    item_loader.add_value('terrace', True)
                item_loader.add_xpath('room_count', '//b[contains(text(), "chambres")]/../../following-sibling::td/text()')
                item_loader.add_value('landlord_name', 'TREVI IMMOCOM')
                item_loader.add_value('landlord_email', 'info@immocom.be')
                item_loader.add_value('landlord_phone', '+32 2 534 86 11')
                item_loader.add_value('external_source', 'Lapropriete_PySpider_france_fr')
                item_loader.add_value('zipcode', zipcode)
                item_loader.add_value('city', city.replace("-"," "))
                yield item_loader.load_item()





             