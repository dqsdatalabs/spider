import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces
import re
import js2xml
import lxml.etree
from parsel import Selector


class ImmofadanSpider(scrapy.Spider):
    
    name = 'immofadan_be'
    allowed_domains = ['immofadan.be']
    start_urls = ['http://www.immofadan.be/index.php']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator='.'
    scale_separator=','
    position = 0
        
    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.immofadan.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&qchambres=&cbien=",
                "property_type": "apartment",
            },
            {
                "url": "http://www.immofadan.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=loft&llocalite=&mprixmin=&mprixmax=&qchambres=&cbien=",
                "property_type": "house",
            },
            {
                "url": "http://www.immofadan.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=mai&llocalite=&mprixmin=&mprixmax=&qchambres=&cbien=",
                "property_type": "house",
            }
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'request_url': url.get("url"),
                                       'property_type': url.get("property_type")})
                
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//a[@class="more-details"]/@href').extract():
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta["property_type"]}
            )

        if len(response.xpath('.//a[@class="more-details"]')) > 0:
            current_page = re.findall(r"(?<=page=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page=)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta["request_url"])
        external_id = response.xpath('.//p[contains(text(),"Réf")]/text()').extract_first()
        item_loader.add_value('external_id', external_id.split(':')[-1])
        item_loader.add_value('property_type', response.meta["property_type"])

        item_loader.add_xpath('title', './/*[@id="breadcrumbs"]/../h2/text()')
        item_loader.add_xpath('description', './/*[@id="desc"]//text()')
        city = response.xpath('.//section[@class="titlebar"]//h2/text()').extract_first()
        item_loader.add_value('city', city.split(' - ')[-1])

        item_loader.add_xpath('rent_string', './/li[contains(text(),"Prix:")]/text()')
        item_loader.add_xpath('utilities', './/li[contains(text(),"Charges:")]/text()')

        javascript = response.xpath('.//script[contains(text(), "myLatlng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            lat_lng = selector.xpath('.//identifier[@name="LatLng"]/../../..//arguments/number/@value').extract()
            if len(lat_lng) == 2:
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])
                
        
        address_str = response.xpath('.//meta[contains(@content,"name")]/@content').extract_first()
        if address_str:
            add_str = remove_white_spaces(address_str.rstrip("name='"))
            item_loader.add_value('address',', '.join(add_str.split(',')[-2:]))
        item_loader.add_xpath('images', './/div[@class="fotorama"]/img/@src')
        # dishwasher
        # http://www.immofadan.be/fr/annonces-immobilieres/location-non-meublee/bruxelles/appartement/2613/sablon--appart-1ch-entierement-renove.html
        if response.xpath('.//p[contains(text(), "Lave vaisselle ")]').extract_first():
            item_loader.add_value("dishwasher", True)

        # elevator
        # http://www.immofadan.be/fr/annonces-immobilieres/location-non-meublee/uccle/appartement/2679/messidor--appart-2-ch-de---90m-.html
        if response.xpath('.//p[contains(text(), "Ascenseur")]').extract_first():
            item_loader.add_value('elevator', True)

        # http://www.immofadan.be/fr/annonces-immobilieres/location-non-meublee/uccle/studio/2658/parvis-de-saint-pierre--studio-de---30m--.html
        item_loader.add_xpath('energy_label', './/strong[contains(text(), "Consommation énergétique:")]/../img/following-sibling::text()')
        room_count = response.xpath('.//li[contains(text(),"Chambre")]/text()').extract_first()
        if room_count and str(int(extract_number_only(room_count)))!='0':
            item_loader.add_value('room_count', extract_number_only(room_count))
        item_loader.add_xpath('square_meters', './/li[contains(text(),"habitable")]/text()')

        # furnished
        furnish = response.xpath('.//*[(@id="desc")]/h3/text()').extract_first().split(' - ')[0]
        if "non meublée" in furnish:
            item_loader.add_value('furnished', False)
        elif "meublée" in furnish:
            item_loader.add_value('furnished', True)

        # bathroom_count
        bathroom_count = response.xpath('.//li[contains(text(),"de bains")]/text()').extract_first()
        shower_count = response.xpath('.//li[contains(text(),"de douche")]/text()').extract_first()
        if bathroom_count and str(int(extract_number_only(bathroom_count)))!='0':
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))
        elif shower_count and str(int(extract_number_only(shower_count)))!='0':
            item_loader.add_value('bathroom_count', extract_number_only(shower_count))
            

        # terrace
        terrace = response.xpath('.//li[contains(text(),"Terrasse")]/text()').extract_first()
        if terrace and terrace not in 'Non':
            item_loader.add_value('terrace', True)

        item_loader.add_value("external_source", "Immofadan_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_name', 'IMMO FADAN sprl')
        item_loader.add_value('landlord_email', 'info@immofadan.be')
        item_loader.add_value('landlord_phone', '32 2 343 42 77')
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
