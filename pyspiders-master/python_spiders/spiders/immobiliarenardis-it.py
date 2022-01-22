# Author Abdulrahman Abbas


import scrapy

from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_lat_long, remove_white_spaces, extract_last_number_only, extract_location_from_coordinates, string_found


class NardisImmobiliare(scrapy.Spider):
    name = 'immobiliarenardis_it'
    allowed_domains = ['immobiliarenardis.it']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = 'NardisImmobiliare_PySpider_Italy'
    start_urls = [
        'https://immobiliarenardis.it/component/osproperty/ricerca-avanzata?property_type=3&agent_type=-1&min_price=0&max_price=2000000&keyword=&sortby=a.price&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&internet=&riscaldamento=&type_costruito=%3D&costruito=&portineria=&giardino=&balcone=&parcheggio=&piscina=&stato=&cantina=&rampa=&advfieldLists=90%2C97%2C88%2C104%2C105%2C106%2C107%2C108%2C109%2C110%2C111&listviewtype=1&currency_item=&live_site=https%3A%2F%2Fimmobiliarenardis.it%2F&current_picture_119=1&current_picture_129=1&current_picture_130=1&current_picture_126=1&current_picture_137=1&current_picture_128=1&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=435&search_param=type%3A1_country%3A92_sortby%3Aa.price_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=90%2C88%2C97%2C104%2C105%2C106%2C108%2C109%2C110%2C107%2C111&advtype_id_3=90%2C88%2C97%2C104%2C105%2C106%2C108%2C109%2C110%2C107%2C111&advtype_id_7=90%2C88%2C97%2C104%2C105%2C106%2C108%2C109%2C110%2C107%2C111&advtype_id_8=']
    position = 1

    def parse(self, response):
        page_links = response.css('h3.clearfix a')
        yield from response.follow_all(page_links, self.parse_info)

    def parse_info(self, response):

        item_loader = ListingLoader(response=response)

        title = remove_white_spaces(response.xpath('//h1[@class="inlineblockdisplay"]//text()').get()).split(",")
        description = response.xpath('//div[@class="row-fluid"]/div[@class="span12"]/text()').getall()
        all_desc = "".join(description)

        address = response.xpath('//div[@class="address_details"]/text()').get()
        city = address.split(",")
        position = extract_lat_long(response.xpath('head/script[30]//text()').get())

        images = response.xpath('//ul[@class="favs"]//img/@src').getall()
        rent = extract_number_only(response.xpath('//div[@id="currency_div"]//text()').get())

        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", extract_number_only(title[0]))
        item_loader.add_value("position", self.position)

        item_loader.add_value('title', title[1:])
        item_loader.add_value('description', description)

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        item_loader.add_value('address', address)
        item_loader.add_value('city', city[-1])
        item_loader.add_value('longitude', position[1])
        item_loader.add_value('latitude', position[0])
        item_loader.add_value('zipcode', extract_location_from_coordinates(position[1], position[0])[0])

        item_loader.add_xpath('room_count', '//div[@class="span12 noleftpadding"]//div[@class="row-fluid"][3]//text()')
        item_loader.add_xpath('bathroom_count', '//div[@class="span12 noleftpadding"]//div[@class="row-fluid"][4]//text()')

        item_loader.add_value('currency', "EUR")
        item_loader.add_value('rent', rent)

        if all_desc.find("mq") != -1:
            locat_square_meters = all_desc.rpartition("mq")
            item_loader.add_value('square_meters', extract_last_number_only(locat_square_meters[0]))
        else:
            item_loader.add_value('square_meters', None)

        washing_machine = ['lavatrice']
        item_loader.add_value('washing_machine', string_found(washing_machine, all_desc))

        elevator = ['ascensore']
        item_loader.add_value('elevator', string_found(elevator, all_desc))

        balcony = ['balcone', 'Balcone']
        item_loader.add_value('balcony', string_found(balcony, all_desc))

        parking = ['posto auto', 'garage', 'un box auto', 'posti auto al coperto']
        item_loader.add_value('parking', string_found(parking, all_desc))

        terrace = ['terrazzo']
        item_loader.add_value('terrace', string_found(terrace, all_desc))

        furnished = ['arredato', 'finemente arredata', 'Arredato', 'completamente arredato']
        if "Non arredato" in all_desc:
            item_loader.add_value('furnished', False)
        elif string_found(furnished, all_desc):
            item_loader.add_value('furnished', True)
        else:
            item_loader.add_value('furnished', False)

        item_loader.add_value('landlord_name', "Nardis Immobiliare")
        item_loader.add_value('landlord_phone', "01943870665")
        item_loader.add_value('landlord_email', "info@immobiliarenardis.it")

        self.position += 1
        yield item_loader.load_item()




